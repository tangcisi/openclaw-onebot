/**
 * 入站消息处理
 */

import type { OneBotMessage } from "../types.js";
import { getOneBotConfig } from "../config.js";
import {
    getRawText,
    getTextFromSegments,
    getReplyMessageId,
    getTextFromMessageContent,
    isMentioned,
} from "../message.js";
import {
    getRenderMarkdownToPlain,
    getCollapseDoubleNewlines,
    getWhitelistUserIds,
    getBlacklistUserIds,
    getOgImageRenderTheme,
    getNormalModeFlushIntervalMs,
    getNormalModeFlushChars,
    getTriggerKeywords,
    getTriggerMode,
    getRandomReplyProbability,
    getReplyWhenWhitelistDenied,
} from "../config.js";
import { markdownToPlain, collapseDoubleNewlines } from "../markdown.js";
import { markdownToImage } from "../og-image.js";
import {
    sendPrivateMsg,
    sendGroupMsg,
    sendPrivateImage,
    sendGroupImage,
    sendGroupForwardMsg,
    sendPrivateForwardMsg,
    setMsgEmojiLike,
    getMsg,
} from "../connection.js";
import { setActiveReplyTarget, clearActiveReplyTarget, setActiveReplySessionId, setForwardSuppressDelivery, setActiveReplySelfId } from "../reply-context.js";
import { loadPluginSdk, getSdk } from "../sdk.js";
import { handleGroupIncrease } from "./group-increase.js";
import { initDebugLog, debugLog } from "../debug-log.js";

const DEFAULT_HISTORY_LIMIT = 20;
export const sessionHistories = new Map<string, Array<{ sender: string; body: string; timestamp: number; messageId: string }>>();

/**
 * 检查消息是否匹配触发关键词
 * @param text 消息文本
 * @param keywords 关键词列表
 * @param mode 匹配模式：prefix-前缀匹配，contains-包含匹配
 */
function checkTriggerKeyword(text: string, keywords: string[], mode: "prefix" | "contains"): boolean {
    if (!text || keywords.length === 0) return false;

    for (const keyword of keywords) {
        if (!keyword) continue;

        if (mode === "prefix") {
            // 前缀匹配：消息以关键词开头（忽略前导空格）
            const trimmedText = text.trimStart();
            if (trimmedText.toLowerCase().startsWith(keyword.toLowerCase())) {
                return true;
            }
        } else {
            // 包含匹配：消息中包含关键词即可
            if (text.toLowerCase().includes(keyword.toLowerCase())) {
                return true;
            }
        }
    }
    return false;
}

/** forward 模式下待处理的会话，用于定期清理未完成的缓冲 */
const forwardPendingSessions = new Map<string, number>();
/** 每个 replySessionId 已发送的 chunk 数量，用于支持多次 final（如工具调用后追加内容） */
const lastSentChunkCountBySession = new Map<string, number>();
const FORWARD_PENDING_TTL_MS = 5 * 60 * 1000; // 5 分钟
const FORWARD_CLEANUP_INTERVAL_MS = 60 * 1000; // 每分钟清理一次

function cleanupForwardPendingSessions(): void {
    const now = Date.now();
    const toDelete: string[] = [];
    for (const [id, ts] of forwardPendingSessions) {
        if (now - ts > FORWARD_PENDING_TTL_MS) toDelete.push(id);
    }
    for (const id of toDelete) forwardPendingSessions.delete(id);
}

let forwardCleanupTimer: ReturnType<typeof setInterval> | null = null;
export function startForwardCleanupTimer(): void {
    if (forwardCleanupTimer) return;
    forwardCleanupTimer = setInterval(cleanupForwardPendingSessions, FORWARD_CLEANUP_INTERVAL_MS);
}

export async function processInboundMessage(api: any, msg: OneBotMessage): Promise<void> {
    await loadPluginSdk();
    const { buildPendingHistoryContextFromMap, recordPendingHistoryEntry, clearHistoryEntriesIfEnabled } = getSdk();

    // 初始化文件 debug 日志（幂等，只在首次生效）
    initDebugLog(api?.config);

    const runtime = api.runtime;
    if (!runtime?.channel?.reply?.dispatchReplyWithBufferedBlockDispatcher) {
        api.logger?.warn?.("[onebot] runtime.channel.reply not available");
        debugLog.warn("runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher not available — message dropped");
        return;
    }

    const config = getOneBotConfig(api);
    if (!config) {
        api.logger?.warn?.("[onebot] not configured");
        debugLog.warn("getOneBotConfig returned null — message dropped");
        return;
    }

    const selfId = msg.self_id ?? 0;
    if (msg.user_id != null && Number(msg.user_id) === Number(selfId)) {
        debugLog.info(`self-message detected (user_id=${msg.user_id} === self_id=${selfId}) — skipped`);
        return;
    }

    const replyId = getReplyMessageId(msg);
    let messageText: string;
    if (replyId != null) {
        const userText = getTextFromSegments(msg);
        try {
            const quoted = await getMsg(replyId);
            const quotedText = quoted ? getTextFromMessageContent(quoted.message) : "";
            const senderLabel = quoted?.sender?.nickname ?? quoted?.sender?.user_id ?? "某人";
            messageText = quotedText.trim()
                ? `[引用 ${String(senderLabel)} 的消息：${quotedText.trim()}]\n${userText}`
                : userText;
        } catch {
            messageText = userText;
        }
    } else {
        messageText = getRawText(msg);
    }
    if (!messageText?.trim()) {
        api.logger?.info?.(`[onebot] ignoring empty message`);
        debugLog.info(`empty message from user=${msg.user_id} type=${msg.message_type} — skipped`);
        return;
    }

    const isGroup = msg.message_type === "group";
    const cfg = api.config;
    const requireMention = (cfg?.channels?.onebot as any)?.requireMention ?? true;

    debugLog.lazy("INFO", () => `incoming: user=${msg.user_id} group=${msg.group_id ?? "N/A"} type=${msg.message_type} selfId=${selfId} text="${messageText.slice(0, 80)}"`);

    // 触发检查逻辑：@提及 和 关键词可同时生效，任一命中即触发；均未命中时可按概率随机回复
    if (isGroup) {
        const isAtMentioned = isMentioned(msg, selfId);
        const triggerKeywords = getTriggerKeywords(cfg);
        const randomReplyProb = getRandomReplyProbability(cfg);

        if (triggerKeywords.length > 0) {
            // 配置了关键词：@ 或关键词任一匹配即触发
            const triggerMode = getTriggerMode(cfg);
            const textFromMsg = getTextFromSegments(msg).trim() || messageText.trim();
            const keywordMatched = checkTriggerKeyword(textFromMsg, triggerKeywords, triggerMode);

            if (!isAtMentioned && !keywordMatched) {
                // 既没 @ 也没关键词命中，检查随机回复概率
                if (randomReplyProb > 0 && Math.random() < randomReplyProb) {
                    api.logger?.info?.(`[onebot] triggered by random reply (probability: ${randomReplyProb})`);
                } else {
                    api.logger?.info?.(`[onebot] ignoring group message: no @mention and no keyword match`);
                    return;
                }
            } else {
                if (isAtMentioned) api.logger?.info?.(`[onebot] triggered by @mention`);
                if (keywordMatched) api.logger?.info?.(`[onebot] triggered by keyword match`);
            }
        } else if (requireMention) {
            // 没配关键词 + requireMention: true → 必须 @
            if (!isAtMentioned) {
                // 没被 @，检查随机回复概率
                if (randomReplyProb > 0 && Math.random() < randomReplyProb) {
                    api.logger?.info?.(`[onebot] triggered by random reply (probability: ${randomReplyProb})`);
                } else {
                    api.logger?.info?.(`[onebot] ignoring group message without @mention`);
                    return;
                }
            }
        }
        // 没配关键词 + requireMention: false → 所有消息都响应（保持原有行为）
    }

    const gi = (cfg?.channels?.onebot as Record<string, unknown>)?.groupIncrease as Record<string, unknown> | undefined;
    // 测试欢迎：@ 机器人并发送 /group-increase，模拟当前发送者入群，触发欢迎（使用该人的 id、nickname 等）
    // 使用 getTextFromSegments 提取纯文本，避免 raw_message 中 [CQ:at,qq=xxx] 等 CQ 码导致匹配失败
    const cmdText = getTextFromSegments(msg).trim() || messageText.trim();
    const groupIncreaseTrigger = isGroup && isMentioned(msg, selfId) && /^\/group-increase\s*$/i.test(cmdText) && gi?.enabled;
    if (groupIncreaseTrigger) {
        const fakeMsg = {
            post_type: "notice",
            notice_type: "group_increase",
            group_id: msg.group_id,
            user_id: msg.user_id,
        } as OneBotMessage;
        await handleGroupIncrease(api, fakeMsg);
        return;
    }

    const userId = msg.user_id!;

    // 白名单检查
    const whitelist = getWhitelistUserIds(cfg);
    if (whitelist.length > 0 && !whitelist.includes(Number(userId))) {
        debugLog.info(`user ${userId} not in whitelist [${whitelist.join(",")}] — denied`);
        if (getReplyWhenWhitelistDenied(cfg)) {
            const denyMsg = "权限不足，请向管理员申请权限";
            const getConfig = () => getOneBotConfig(api);
            try {
                if (msg.message_type === "group" && msg.group_id) await sendGroupMsg(msg.group_id, denyMsg, getConfig);
                else await sendPrivateMsg(userId, denyMsg, getConfig);
            } catch (_) { }
        }
        api.logger?.info?.(`[onebot] user ${userId} not in whitelist, denied`);
        return;
    }

    // 黑名单检查
    const blacklist = getBlacklistUserIds(cfg);
    if (blacklist.length > 0 && blacklist.includes(Number(userId))) {
        api.logger?.info?.(`[onebot] user ${userId} is in blacklist, ignored`);
        debugLog.info(`user ${userId} in blacklist — ignored`);
        return;
    }
    const groupId = msg.group_id;
    const tempSessionId = isGroup
        ? `onebot:group:${groupId}`.toLowerCase()
        : `onebot:${userId}`.toLowerCase();

    const route = runtime.channel.routing?.resolveAgentRoute?.({
        cfg,
        sessionKey: tempSessionId,
        channel: "onebot",
        accountId: config.accountId ?? "default",
    }) ?? { agentId: "main" };

    // 修复构造符合 OpenClaw 规范的全局 SessionKey格式必须为 agent:{agentId}:{channel}:{type}:{id}，否则下方的dispatchReplyWithBufferedBlockDispatcher会触发自动兜底机制，直接在 main 代理下“克隆”出一个一模一样的会话，导致多agent配置达不到效果
    const sessionId = `agent:${route.agentId}:${tempSessionId}`;
    const storePath =
        runtime.channel.session?.resolveStorePath?.(cfg?.session?.store, {
            agentId: route.agentId,
        }) ?? "";

    const envelopeOptions = runtime.channel.reply?.resolveEnvelopeFormatOptions?.(cfg) ?? {};
    const chatType = isGroup ? "group" : "direct";
    // 优先使用群名片(card)，其次是昵称(nickname)，都没有则为空串
    const senderNickname = (isGroup ? msg.sender?.card?.trim() : undefined)
        || msg.sender?.nickname?.trim()
        || "";
    const fromLabel = senderNickname || String(userId);

    // 添加日志：打印插件接收到的原始消息内容
    api.logger?.info?.(`[onebot] received message from user ${userId}: "${messageText}"`);

    const formattedBody =
        runtime.channel.reply?.formatInboundEnvelope?.({
            channel: "OneBot",
            from: fromLabel,
            timestamp: Date.now(),
            body: messageText,
            chatType,
            sender: { name: fromLabel, id: String(userId) },
            envelope: envelopeOptions,
        }) ?? { content: [{ type: "text", text: messageText }] };

    const body = buildPendingHistoryContextFromMap
        ? buildPendingHistoryContextFromMap({
            historyMap: sessionHistories,
            historyKey: sessionId,
            limit: DEFAULT_HISTORY_LIMIT,
            currentMessage: formattedBody,
            formatEntry: (entry: any) =>
                runtime.channel.reply?.formatInboundEnvelope?.({
                    channel: "OneBot",
                    from: fromLabel,
                    timestamp: entry.timestamp,
                    body: entry.body,
                    chatType,
                    senderLabel: entry.sender,
                    envelope: envelopeOptions,
                }) ?? { content: [{ type: "text", text: entry.body }] },
        })
        : formattedBody;

    if (recordPendingHistoryEntry) {
        recordPendingHistoryEntry({
            historyMap: sessionHistories,
            historyKey: sessionId,
            entry: {
                sender: fromLabel,
                body: messageText,
                timestamp: Date.now(),
                messageId: `onebot-${Date.now()}`,
            },
            limit: DEFAULT_HISTORY_LIMIT,
        });
    }

    // 回复目标（参考 openclaw-feishu）：群聊用 group:群号，私聊用 user:用户号
    // To / OriginatingTo / ConversationLabel 均表示「发送目标」，Agent 的 message 工具会据此选择 target
    const replyTarget = isGroup ? `onebot:group:${groupId}` : `onebot:${userId}`;
    const ctxPayload = {
        Body: body,
        RawBody: messageText,
        From: isGroup ? `onebot:group:${groupId}` : `onebot:${userId}`,
        To: replyTarget,
        SessionKey: sessionId,
        AccountId: config.accountId ?? "default",
        ChatType: chatType,
        ConversationLabel: replyTarget, // 与 Feishu 一致：表示会话/回复目标，群聊时为 group:群号，非 SenderId
        SenderName: fromLabel,
        SenderId: String(userId),
        Provider: "onebot",
        Surface: "onebot",
        MessageSid: `onebot-${Date.now()}`,
        Timestamp: Date.now(),
        OriginatingChannel: "onebot",
        OriginatingTo: replyTarget,
        CommandAuthorized: true,
        DeliveryContext: {
            channel: "onebot",
            to: replyTarget,
            accountId: config.accountId ?? "default",
        },
        _onebot: { userId, groupId, isGroup },
    };

    if (runtime.channel.session?.recordInboundSession) {
        await runtime.channel.session.recordInboundSession({
            storePath,
            sessionKey: sessionId,
            ctx: ctxPayload,
            updateLastRoute: !isGroup ? { sessionKey: sessionId, channel: "onebot", to: String(userId), accountId: config.accountId ?? "default" } : undefined,
            onRecordError: (err: any) => api.logger?.warn?.(`[onebot] recordInboundSession: ${err}`),
        });
    }

    if (runtime.channel.activity?.record) {
        runtime.channel.activity.record({ channel: "onebot", accountId: config.accountId ?? "default", direction: "inbound" });
    }

    const onebotCfg = (cfg?.channels?.onebot as Record<string, unknown>) ?? {};
    const thinkingEmojiId = (onebotCfg.thinkingEmojiId as number) ?? 60;
    const userMessageId = msg.message_id;

    let emojiAdded = false;
    const clearEmojiReaction = async () => {
        if (emojiAdded && userMessageId != null) {
            try {
                await setMsgEmojiLike(userMessageId, thinkingEmojiId, false);
            } catch { }
            emojiAdded = false;
        }
    };

    if (userMessageId != null) {
        try {
            await setMsgEmojiLike(userMessageId, thinkingEmojiId, true);
            emojiAdded = true;
        } catch {
            api.logger?.warn?.("[onebot] setMsgEmojiLike failed (maybe OneBot doesn't support it)");
        }
    }

    const traceId = `trace-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const traceLog = {
        info: (m: string) => api.logger?.info?.(`[${traceId}] ${m}`),
        warn: (m: string) => api.logger?.warn?.(`[${traceId}] ${m}`),
        error: (m: string) => api.logger?.error?.(`[${traceId}] ${m}`)
    };

    traceLog.info(`dispatching message for session ${sessionId}`);

    const longMessageMode = (onebotCfg.longMessageMode as "normal" | "og_image" | "forward") ?? "normal";
    const longMessageThreshold = (onebotCfg.longMessageThreshold as number) ?? 300;
    traceLog.info(`longMessageMode=${longMessageMode}, threshold=${longMessageThreshold}`);
    const normalModeFlushIntervalMs = getNormalModeFlushIntervalMs(cfg);
    const normalModeFlushChars = getNormalModeFlushChars(cfg);

    const replySessionId = `onebot-reply-${Date.now()}-${sessionId}`;
    setActiveReplyTarget(replyTarget);
    setActiveReplySessionId(replySessionId);
    setActiveReplySelfId(selfId);
    if (longMessageMode === "forward") setForwardSuppressDelivery(true);

    const deliveredChunks: Array<{ index: number; text?: string; rawText?: string; mediaUrl?: string }> = [];
    let chunkIndex = 0;
    let normalModeBufferedText = "";
    let normalModeBufferedRawText = "";
    let normalModeFlushTimer: ReturnType<typeof setTimeout> | null = null;
    let normalModeFlushChain: Promise<void> = Promise.resolve();
    let receivedFinal = false;

    const getConfig = () => getOneBotConfig(api);

    const onReplySessionEnd = onebotCfg.onReplySessionEnd as string | ((ctx: ReplySessionContext) => void | Promise<void>) | undefined;
    const normalModePunctuationFlushMinChars = 24;

    const clearNormalModeFlushTimer = (reason: string = "unknown") => {
        if (normalModeFlushTimer) {
            traceLog.info(`clearNormalModeFlushTimer: clearing timer, reason=${reason}`);
            clearTimeout(normalModeFlushTimer);
            normalModeFlushTimer = null;
        }
    };

    const hasBufferedNormalModeText = () => normalModeBufferedText.length > 0 || normalModeBufferedRawText.length > 0;

    const queueNormalModeFlush = (action: () => Promise<void>): Promise<void> => {
        normalModeFlushChain = normalModeFlushChain
            .then(action)
            .catch((e: any) => {
                traceLog.error(`normal-mode flush failed: ${e?.message ?? e}`);
            });
        return normalModeFlushChain;
    };

    const doSendChunk = async (
        effectiveIsGroup: boolean,
        effectiveGroupId: number | undefined,
        uid: number | undefined,
        text: string,
        mediaUrl: string | undefined
    ) => {
        if (text) {
            try {
                if (effectiveIsGroup && effectiveGroupId) {
                    await sendGroupMsg(effectiveGroupId, text, getConfig);
                    debugLog.lazy("INFO", () => `sendGroupMsg OK: group=${effectiveGroupId} len=${text.length}`);
                } else if (uid) {
                    await sendPrivateMsg(uid, text, getConfig);
                    debugLog.lazy("INFO", () => `sendPrivateMsg OK: user=${uid} len=${text.length}`);
                } else {
                    debugLog.warn(`doSendChunk: text present but no valid target (isGroup=${effectiveIsGroup} groupId=${effectiveGroupId} uid=${uid}) — message lost!`);
                }
            } catch (e: any) {
                debugLog.error(`doSendChunk text failed: ${e?.message ?? e}`);
                throw e;
            }
        }
        if (mediaUrl) {
            try {
                if (effectiveIsGroup && effectiveGroupId) {
                    await sendGroupImage(effectiveGroupId, mediaUrl, api.logger, getConfig);
                    debugLog.lazy("INFO", () => `sendGroupImage OK: group=${effectiveGroupId}`);
                } else if (uid) {
                    await sendPrivateImage(uid, mediaUrl, api.logger, getConfig);
                    debugLog.lazy("INFO", () => `sendPrivateImage OK: user=${uid}`);
                } else {
                    debugLog.warn(`doSendChunk: mediaUrl present but no valid target — media lost!`);
                }
            } catch (e: any) {
                debugLog.error(`doSendChunk media failed: ${e?.message ?? e}`);
                throw e;
            }
        }
    };

    const flushBufferedNormalModeText = async (
        effectiveIsGroup: boolean,
        effectiveGroupId: number | undefined,
        uid: number | undefined
    ): Promise<void> => {
        clearNormalModeFlushTimer("flushBufferedNormalModeText");
        if (!hasBufferedNormalModeText()) return;

        const text = normalModeBufferedText;
        const rawText = normalModeBufferedRawText;
        traceLog.info(`flushBufferedNormalModeText: textLen=${text.length}, textPreview="${text.slice(0, 30).replace(/\n/g, '\\n')}"`);
        normalModeBufferedText = "";
        normalModeBufferedRawText = "";

        await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, text, undefined);
        deliveredChunks.push({
            index: chunkIndex++,
            text: text || undefined,
            rawText: rawText || undefined,
        });
    };

    const scheduleNormalModeFlush = (
        effectiveIsGroup: boolean,
        effectiveGroupId: number | undefined,
        uid: number | undefined
    ) => {
        if (normalModeFlushTimer) return;
        traceLog.info(`scheduleNormalModeFlush: scheduled (interval=${normalModeFlushIntervalMs}ms)`);
        normalModeFlushTimer = setTimeout(() => {
            traceLog.info(`scheduleNormalModeFlush: timer triggered`);
            void queueNormalModeFlush(() => flushBufferedNormalModeText(effectiveIsGroup, effectiveGroupId, uid));
        }, normalModeFlushIntervalMs);
    };

    const shouldFlushNormalModeBuffer = (): boolean => {
        const rawText = normalModeBufferedRawText || normalModeBufferedText;
        if (!rawText) return false;
        if (normalModeBufferedText.length >= normalModeFlushChars) return true;
        if (rawText.length < normalModePunctuationFlushMinChars) return false;
        return /[.!?。！？]\s*$/.test(rawText);
    };

    const appendNormalModeText = (current: string, next: string): string => {
        if (!current) return next;
        if (!next) return current;

        const lastChar = current[current.length - 1];
        const firstChar = next[0];
        if (/[A-Za-z0-9]/.test(lastChar) && /[A-Za-z0-9]/.test(firstChar)) {
            return `${current} ${next}`;
        }
        return `${current}${next}`;
    };

    try {
        await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
            ctx: ctxPayload,
            cfg,
            dispatcherOptions: {
                deliver: async (payload: unknown, info: { kind: string }) => {
                    await clearEmojiReaction();

                    const p = payload as { text?: string; body?: string; mediaUrl?: string; mediaUrls?: string[] } | string;
                    const replyText = typeof p === "string" ? p : (p?.text ?? p?.body ?? "");
                    const mediaUrl = typeof p === "string" ? undefined : (p?.mediaUrl ?? p?.mediaUrls?.[0]);
                    const trimmed = (replyText || "").trim();

                    traceLog.info(`deliver entry: kind=${info.kind}, textLen=${replyText.length}, mediaUrl=${!!mediaUrl}, deliveredChunks=${deliveredChunks.length}`);

                    if (info.kind === "final") {
                        receivedFinal = true;
                    }

                    if ((!trimmed || trimmed === "NO_REPLY" || trimmed.endsWith("NO_REPLY")) && !mediaUrl) {
                        debugLog.lazy("INFO", () => `deliver: suppressed (NO_REPLY or empty), kind=${info.kind}, raw="${replyText.slice(0, 60)}"`);
                        return;
                    }

                    const { userId: uid, groupId: gid, isGroup: ig } = (ctxPayload as any)._onebot || {};
                    const sessionKey = String((ctxPayload as any).SessionKey ?? sessionId);
                    const groupMatch = sessionKey.match(/^onebot:group:(\d+)$/i);
                    const effectiveIsGroup = groupMatch != null || Boolean(ig);
                    const effectiveGroupId = (groupMatch ? parseInt(groupMatch[1], 10) : undefined) ?? gid;

                    const usePlain = getRenderMarkdownToPlain(cfg);
                    let textPlain = usePlain ? markdownToPlain(trimmed) : trimmed;
                    if (getCollapseDoubleNewlines(cfg)) textPlain = collapseDoubleNewlines(textPlain);

                    const shouldSendNow = longMessageMode === "normal";

                    if (!shouldSendNow) {
                        deliveredChunks.push({
                            index: chunkIndex++,
                            text: textPlain || undefined,
                            rawText: trimmed || undefined,
                            mediaUrl: mediaUrl || undefined,
                        });
                    }

                    // forward 模式且非最后一条：仅暂存，绝不发送，等 final 时再统一处理
                    if (longMessageMode === "forward" && info.kind !== "final") {
                        forwardPendingSessions.set(replySessionId, Date.now());
                        return;
                    }
                    if (info.kind === "final" && longMessageMode === "forward") {
                        forwardPendingSessions.delete(replySessionId);
                    }

                    try {
                        if (shouldSendNow) {
                            if (mediaUrl) {
                                await queueNormalModeFlush(async () => {
                                    await flushBufferedNormalModeText(effectiveIsGroup, effectiveGroupId, uid);
                                    await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, textPlain, mediaUrl);
                                    deliveredChunks.push({
                                        index: chunkIndex++,
                                        text: textPlain || undefined,
                                        rawText: trimmed || undefined,
                                        mediaUrl: mediaUrl || undefined,
                                    });
                                });
                            } else {
                                normalModeBufferedText = appendNormalModeText(normalModeBufferedText, textPlain);
                                normalModeBufferedRawText = appendNormalModeText(normalModeBufferedRawText, trimmed);

                                if (shouldFlushNormalModeBuffer()) {
                                    await queueNormalModeFlush(() => flushBufferedNormalModeText(effectiveIsGroup, effectiveGroupId, uid));
                                } else {
                                    scheduleNormalModeFlush(effectiveIsGroup, effectiveGroupId, uid);
                                }
                            }
                        }
                        if (info.kind === "final") {
                            if (shouldSendNow) {
                                await queueNormalModeFlush(() => flushBufferedNormalModeText(effectiveIsGroup, effectiveGroupId, uid));
                            }

                            const lastSentCount = lastSentChunkCountBySession.get(replySessionId) ?? 0;
                            const chunksToSend = deliveredChunks.slice(lastSentCount);
                            if (chunksToSend.length === 0) return;

                            const totalLen = deliveredChunks.reduce((s, c) => s + (c.rawText ?? c.text ?? "").length, 0);
                            const incrementalLen = chunksToSend.reduce((s, c) => s + (c.rawText ?? c.text ?? "").length, 0);
                            const isLong = totalLen > longMessageThreshold;
                            const isIncrementalLong = incrementalLen > longMessageThreshold;
                            const isIncremental = lastSentCount > 0;
                            traceLog.info(`final check: totalLen=${totalLen}, threshold=${longMessageThreshold}, isLong=${isLong}, isIncremental=${isIncremental}, deliveredChunks=${deliveredChunks.length}`);

                            if (isIncremental) {
                                setForwardSuppressDelivery(false);
                                // normal 模式下增量 chunk 已在 deliver 中实时发出；这里不能在 final 再补发一次。
                                if (!shouldSendNow && isIncrementalLong && (longMessageMode === "og_image" || longMessageMode === "forward")) {
                                    const fullRaw = chunksToSend.map((c) => c.rawText ?? c.text ?? "").join("\n\n");
                                    if (fullRaw.trim()) {
                                        if (longMessageMode === "og_image") {
                                            try {
                                                const imgUrl = await markdownToImage(fullRaw, { theme: getOgImageRenderTheme(api?.config) });
                                                if (imgUrl) {
                                                    if (effectiveIsGroup && effectiveGroupId) await sendGroupImage(effectiveGroupId, imgUrl, api.logger, getConfig);
                                                    else if (uid) await sendPrivateImage(uid, imgUrl, api.logger, getConfig);
                                                } else {
                                                    api.logger?.warn?.("[onebot] og_image (incremental): satori or sharp not installed, falling back to normal send");
                                                    for (const c of chunksToSend) {
                                                        if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                                    }
                                                }
                                            } catch (e: any) {
                                                api.logger?.error?.(`[onebot] og_image (incremental) failed: ${e?.message}`);
                                                for (const c of chunksToSend) {
                                                    if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                                }
                                            }
                                        } else {
                                            try {
                                                const nodes: Array<{ type: string; data: Record<string, unknown> }> = [];
                                                for (const c of chunksToSend) {
                                                    if (c.mediaUrl) {
                                                        const mid = await sendPrivateImage(selfId, c.mediaUrl, api.logger, getConfig);
                                                        if (mid) nodes.push({ type: "node", data: { id: String(mid) } });
                                                    } else if (c.text) {
                                                        const mid = await sendPrivateMsg(selfId, c.text, getConfig);
                                                        if (mid) nodes.push({ type: "node", data: { id: String(mid) } });
                                                    }
                                                }
                                                if (nodes.length > 0) {
                                                    if (effectiveIsGroup && effectiveGroupId) await sendGroupForwardMsg(effectiveGroupId, nodes, getConfig);
                                                    else if (uid) await sendPrivateForwardMsg(uid, nodes, getConfig);
                                                } else {
                                                    for (const c of chunksToSend) {
                                                        if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                                    }
                                                }
                                            } catch (e: any) {
                                                api.logger?.error?.(`[onebot] forward (incremental) failed: ${e?.message}`);
                                                for (const c of chunksToSend) {
                                                    if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                                }
                                            }
                                        }
                                    } else {
                                        for (const c of chunksToSend) {
                                            if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                        }
                                    }
                                } else if (!shouldSendNow) {
                                    for (const c of chunksToSend) {
                                        if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                    }
                                }
                            } else if (!shouldSendNow && (longMessageMode === "og_image" || longMessageMode === "forward")) {
                                traceLog.info(`checking og_image: isLong=${isLong}, mode=${longMessageMode}`);
                                if (isLong && longMessageMode === "og_image") {
                                    traceLog.info(`triggering og_image for ${totalLen} chars`);
                                    const fullRaw = deliveredChunks.map((c) => c.rawText ?? c.text ?? "").join("\n\n");
                                    if (fullRaw.trim()) {
                                        try {
                                            const imgUrl = await markdownToImage(fullRaw, { theme: getOgImageRenderTheme(api?.config) });
                                            if (imgUrl) {
                                                if (effectiveIsGroup && effectiveGroupId) await sendGroupImage(effectiveGroupId, imgUrl, api.logger, getConfig);
                                                else if (uid) await sendPrivateImage(uid, imgUrl, api.logger, getConfig);
                                            } else {
                                                api.logger?.warn?.("[onebot] og_image: satori or sharp not installed, falling back to normal send");
                                                setForwardSuppressDelivery(false);
                                                for (const c of deliveredChunks) {
                                                    if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                                }
                                            }
                                        } catch (e: any) {
                                            api.logger?.error?.(`[onebot] og_image failed: ${e?.message}`);
                                            setForwardSuppressDelivery(false);
                                            for (const c of deliveredChunks) {
                                                if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                            }
                                        }
                                    }
                                } else if (isLong && longMessageMode === "forward") {
                                    try {
                                        const nodes: Array<{ type: string; data: Record<string, unknown> }> = [];
                                        for (const c of deliveredChunks) {
                                            if (c.mediaUrl) {
                                                const mid = await sendPrivateImage(selfId, c.mediaUrl, api.logger, getConfig);
                                                if (mid) nodes.push({ type: "node", data: { id: String(mid) } });
                                            } else if (c.text) {
                                                const mid = await sendPrivateMsg(selfId, c.text, getConfig);
                                                if (mid) nodes.push({ type: "node", data: { id: String(mid) } });
                                            }
                                        }
                                        if (nodes.length > 0) {
                                            if (effectiveIsGroup && effectiveGroupId) await sendGroupForwardMsg(effectiveGroupId, nodes, getConfig);
                                            else if (uid) await sendPrivateForwardMsg(uid, nodes, getConfig);
                                        } else {
                                            // forward 模式下所有 self-send 都失败，nodes 为空，降级逐条发送
                                            debugLog.warn(`forward: all self-sends failed, nodes empty (${deliveredChunks.length} chunks) — falling back to direct send`);
                                            setForwardSuppressDelivery(false);
                                            for (const c of deliveredChunks) {
                                                if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                            }
                                        }
                                    } catch (e: any) {
                                        api.logger?.error?.(`[onebot] forward failed: ${e?.message}`);
                                        setForwardSuppressDelivery(false);
                                        for (const c of deliveredChunks) {
                                            if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                        }
                                    }
                                } else {
                                    setForwardSuppressDelivery(false);
                                    for (const c of deliveredChunks) {
                                        if (c.text || c.mediaUrl) await doSendChunk(effectiveIsGroup, effectiveGroupId, uid, c.text ?? "", c.mediaUrl);
                                    }
                                }
                            }

                            lastSentChunkCountBySession.set(replySessionId, deliveredChunks.length);

                            if (clearHistoryEntriesIfEnabled) {
                                clearHistoryEntriesIfEnabled({
                                    historyMap: sessionHistories,
                                    historyKey: sessionId,
                                    limit: DEFAULT_HISTORY_LIMIT,
                                });
                            }
                            if (onReplySessionEnd) {
                                const ctx: ReplySessionContext = {
                                    replySessionId,
                                    sessionId,
                                    to: replyTarget,
                                    chunks: deliveredChunks.map(({ index, text: t, mediaUrl: m }) => ({ index, text: t, mediaUrl: m })),
                                    userMessage: messageText,
                                };
                                if (typeof onReplySessionEnd === "function") {
                                    await onReplySessionEnd(ctx);
                                } else if (typeof onReplySessionEnd === "string" && onReplySessionEnd.trim()) {
                                    const { loadScript } = await import("../load-script.js");
                                    const mod = await loadScript(onReplySessionEnd.trim());
                                    const fn = mod?.default ?? mod?.onReplySessionEnd;
                                    if (typeof fn === "function") await fn(ctx);
                                }
                            }
                        }
                    } catch (e: any) {
                        traceLog.error(`deliver failed: ${e?.message}`);
                        debugLog.error(`deliver exception: kind=${info.kind} err=${e?.message ?? e}`);
                    }
                },
                onError: async (err: any, info: any) => {
                    traceLog.error(`${info?.kind} reply failed: ${err}`);
                    debugLog.error(`onError: kind=${info?.kind} err=${err}`);
                    await clearEmojiReaction();
                },
            },
            replyOptions: { disableBlockStreaming: longMessageMode !== "normal" },
        });
        traceLog.info(`dispatchReplyWithBufferedBlockDispatcher returned successfully.`);
        debugLog.lazy("INFO", () => `dispatch complete: session=${sessionId} receivedFinal=${receivedFinal} chunks=${deliveredChunks.length}`);
    } catch (err: any) {
        await clearEmojiReaction();
        // 异常时清空缓冲，避免 finally 补发半截正文后再发错误消息
        traceLog.error(`dispatch catch block: err=${err?.message}, receivedFinal=${receivedFinal}, chunkIndex=${chunkIndex}`);
        debugLog.error(`dispatch exception: err=${err?.message ?? err} receivedFinal=${receivedFinal} chunks=${deliveredChunks.length} session=${sessionId}`);
        normalModeBufferedText = "";
        normalModeBufferedRawText = "";
        try {
            const { userId: uid, groupId: gid, isGroup: ig } = (ctxPayload as any)._onebot || {};
            if (ig && gid) await sendGroupMsg(gid, `处理失败: ${err?.message?.slice(0, 80) || "未知错误"}`);
            else if (uid) await sendPrivateMsg(uid, `处理失败: ${err?.message?.slice(0, 80) || "未知错误"}`);
        } catch (_) { }
    } finally {
        traceLog.info(`dispatch finally block: receivedFinal=${receivedFinal}, hasBuffered=${hasBufferedNormalModeText()}, bufferLen=${normalModeBufferedText.length}, hasTimer=${!!normalModeFlushTimer}, chunks=${deliveredChunks.length}`);
        if (!receivedFinal && deliveredChunks.length === 0) {
            debugLog.warn(`dispatch ended without final and 0 chunks — agent may have produced no output. session=${sessionId}`);
        } else if (!receivedFinal) {
            debugLog.warn(`dispatch ended without final but has ${deliveredChunks.length} chunks — possible timeout. session=${sessionId}`);
        }
        // 补发缓冲池中残留的文本（引擎未发送 final 帧时会走到这里）
        if (hasBufferedNormalModeText()) {
            try {
                const { userId: uid, groupId: gid, isGroup: ig } = (ctxPayload as any)._onebot || {};
                const sessionKey = String((ctxPayload as any).SessionKey ?? sessionId);
                const groupMatch = sessionKey.match(/^onebot:group:(\d+)$/i);
                const effectiveIsGroup = groupMatch != null || Boolean(ig);
                const effectiveGroupId = (groupMatch ? parseInt(groupMatch[1], 10) : undefined) ?? gid;
                queueNormalModeFlush(() => flushBufferedNormalModeText(effectiveIsGroup, effectiveGroupId, uid));
                await normalModeFlushChain;
            } catch (e: any) {
                traceLog.error(`finally flush failed: ${e?.message ?? e}`);
            }
        }
        clearNormalModeFlushTimer("finally");
        setForwardSuppressDelivery(false);
        setActiveReplySelfId(null);
        lastSentChunkCountBySession.delete(replySessionId);
        forwardPendingSessions.delete(replySessionId);
        setActiveReplySessionId(null);
        clearActiveReplyTarget();
    }
}

/** 回复会话上下文，供 onReplySessionEnd 钩子使用 */
export interface ReplySessionContext {
    /** 本次回复会话的唯一 ID，同一用户问题下的多次 deliver 共享此 ID */
    replySessionId: string;
    /** 会话标识，如 onebot:group:123 或 onebot:456 */
    sessionId: string;
    /** 回复目标，如 onebot:group:123 或 onebot:456 */
    to: string;
    /** 本次回复中已发送的所有块（按顺序） */
    chunks: Array<{ index: number; text?: string; mediaUrl?: string }>;
    /** 用户原始消息 */
    userMessage: string;
}
