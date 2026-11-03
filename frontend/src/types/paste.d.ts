import type { PaginationInfo } from "./api";

export interface Paste {
  id: number;
  slug: string;
  content: string;
  title?: string | null;
  remark?: string;
  hasPassword?: boolean;
  plain_password?: string | null;
  requiresPassword?: boolean;
  views?: number;
  max_views?: number | null;
  expires_at?: string | null;
  is_expired?: boolean;
  is_public?: boolean;
  created_by?: string | null;
  created_at?: string;
  updated_at?: string;
  tags?: PasteTag[];
}

export interface PasteTag {
  id: string;
  name: string;
  color: string;
  sort_order?: number;
  usage_count?: number;
}

export interface PasteListResponse {
  items: Paste[];
  pagination: PaginationInfo;
}

export interface PasteUpdatePayload {
  content: string;
  title?: string | null;
  remark?: string;
  max_views?: number | null;
  expires_at?: string | null;
  is_public?: boolean;
  password?: string | null;
  clearPassword?: boolean;
  tag_ids?: string[];
}
