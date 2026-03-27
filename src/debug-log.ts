/**
 * 文件 debug 日志，独立于 api.logger，即使 Gateway 日志不可见也能排查问题。
 *
 * 配置项（channels.onebot）：
 *   debugLog: boolean          — 是否启用文件日志，默认 false
 *   debugLogPath: string       — 日志文件路径，默认 ~/.openclaw/logs/onebot-debug.log
 *   debugLogMaxSizeMB: number  — 单文件最大 MB，超出后轮转，默认 10
 */

import * as fs from "fs";
import * as path from "path";
import * as os from "os";

let _enabled = false;
let _logPath = "";
let _maxBytes = 10 * 1024 * 1024; // 10 MB
let _initialized = false;

export function initDebugLog(cfg: any): void {
    const ob = cfg?.channels?.onebot;
    _enabled = ob?.debugLog === true;
    if (!_enabled) {
        _initialized = true;
        return;
    }
    _logPath =
        typeof ob?.debugLogPath === "string" && ob.debugLogPath.trim()
            ? ob.debugLogPath.trim()
            : path.join(os.homedir(), ".openclaw", "logs", "onebot-debug.log");
    const maxMB = typeof ob?.debugLogMaxSizeMB === "number" ? ob.debugLogMaxSizeMB : 10;
    _maxBytes = Math.max(1, maxMB) * 1024 * 1024;
    try {
        const dir = path.dirname(_logPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    } catch {}
    _initialized = true;
}

function rotate(): void {
    try {
        const stat = fs.statSync(_logPath);
        if (stat.size > _maxBytes) {
            const bak = _logPath + ".1";
            if (fs.existsSync(bak)) fs.unlinkSync(bak);
            fs.renameSync(_logPath, bak);
        }
    } catch {}
}

function write(level: string, msg: string): void {
    if (!_enabled) return;
    try {
        rotate();
        const ts = new Date().toISOString();
        fs.appendFileSync(_logPath, `[${ts}] [${level}] ${msg}\n`);
    } catch {}
}

export const debugLog = {
    info: (msg: string) => write("INFO", msg),
    warn: (msg: string) => write("WARN", msg),
    error: (msg: string) => write("ERROR", msg),
    /** 条件式调用，只有启用了才执行 fn 构造日志字符串（避免无谓的字符串拼接开销） */
    lazy: (level: "INFO" | "WARN" | "ERROR", fn: () => string) => {
        if (!_enabled) return;
        write(level, fn());
    },
    get enabled() { return _enabled; },
    get path() { return _logPath; },
};
