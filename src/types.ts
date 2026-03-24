/**
 * OneBot 协议类型定义
 */

export interface OneBotMessage {
  post_type: string;
  message_type?: "private" | "group";
  message_id?: number;
  user_id?: number;
  group_id?: number;
  message?: Array<{ type: string; data?: Record<string, unknown> }>;
  raw_message?: string;
  self_id?: number;
  time?: number;
  sender?: {
    user_id?: number;
    nickname?: string;
    card?: string;
  };
  [key: string]: unknown;
}

export interface OneBotAccountConfig {
  accountId?: string;
  type: "forward-websocket" | "backward-websocket";
  host: string;
  port: number;
  accessToken?: string;
  path?: string;
  enabled?: boolean;
}
