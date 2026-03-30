export type AuthMode = "account" | "public";

export type User = {
  id: string;
  email: string;
  full_name: string;
  created_at: string;
};

export type AuthResponse = {
  access_token: string;
  token_type: string;
  user: User;
};

export type LoginPayload = {
  email: string;
  password: string;
};

export type SignupPayload = LoginPayload & {
  full_name: string;
};

export type ChartFigure = {
  data: Record<string, unknown>[];
  layout: Record<string, unknown>;
  frames?: Record<string, unknown>[];
};

export type ChartSpec = {
  id: string;
  type: string;
  title: string;
  description: string;
  figure: ChartFigure;
};

export type HistoryItem = {
  id: string;
  job_id?: string | null;
  job_status_url?: string | null;
  dataset_name: string;
  source_type: string;
  target_column: string | null;
  row_count: number;
  column_count: number;
  status: string;
  progress: number;
  progress_message: string | null;
  processing_mode: string | null;
  file_type: string | null;
  file_size_bytes: number | null;
  error_message: string | null;
  share_token: string;
  share_url: string;
  created_at: string;
};

export type JobStatus = {
  job_id: string;
  report_id: string;
  dataset_name: string;
  status: string;
  progress: number;
  message?: string | null;
  progress_message: string | null;
  processing_mode: string | null;
  file_type: string | null;
  file_size_bytes: number | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  failed_at?: string | null;
  result?: ReportPayload | null;
};

export type UploadCompletedPart = {
  part_number: number;
  etag: string;
};

export type UploadPartInstruction = {
  part_number: number;
  url: string;
};

export type UploadSession = {
  upload_id: string;
  upload_strategy: string;
  storage_backend: string;
  storage_key: string;
  processing_mode: string;
  expires_at: string;
  chunk_size_bytes?: number | null;
  single_part_url?: string | null;
  single_part_headers: Record<string, string>;
  multipart_upload_id?: string | null;
  multipart_parts: UploadPartInstruction[];
};

export type UploadSessionStatus = {
  upload_id: string;
  dataset_name: string;
  target_column?: string | null;
  original_filename: string;
  content_type?: string | null;
  file_size_bytes: number;
  processing_mode: string;
  upload_strategy: string;
  storage_backend: string;
  storage_key: string;
  status: string;
  progress: number;
  message?: string | null;
  progress_message?: string | null;
  error_message?: string | null;
  report_id?: string | null;
  job_id?: string | null;
  created_at: string;
  updated_at: string;
  expires_at?: string | null;
  report?: ReportDetail | null;
};

export type ReportColumnProfile = {
  column: string;
  dtype: string;
  missing_count: number;
  missing_percentage: number;
  unique_values: number;
  sample_values: Array<string | number | boolean | null>;
};

export type SummaryStatistic = {
  column: string;
  dtype: string;
  non_null_count: number;
  unique_values: number;
  mean?: number | null;
  median?: number | null;
  std?: number | null;
  min?: number | string | null;
  max?: number | string | null;
  q1?: number | null;
  q3?: number | null;
  top_value?: string | number | null;
  top_frequency?: number;
};

export type CorrelationPair = {
  left_column: string;
  right_column: string;
  correlation: number;
};

export type OutlierSummary = {
  column: string;
  count: number;
  percentage: number;
  lower_bound: number;
  upper_bound: number;
};

export type TrendSummary = {
  column: string;
  direction: string;
  basis: string;
  slope?: number;
  description: string;
};

export type ModelSuggestion = {
  name: string;
  rationale: string;
};

export type FeatureImportance = {
  feature: string;
  importance: number;
};

export type ModelingSummary = {
  status: string;
  mode: string;
  target_column?: string;
  selected_model?: string;
  suggestions: ModelSuggestion[];
  metrics?: Record<string, number>;
  feature_importance?: FeatureImportance[];
  class_distribution?: Record<string, number>;
  cluster_summary?: Record<string, number>;
  notes?: string;
  reason?: string;
};

export type ReportSectionStatus = {
  overview: boolean;
  summary_statistics: boolean;
  correlations: boolean;
  outliers: boolean;
  trends: boolean;
  charts: boolean;
  rows: boolean;
  modeling: boolean;
  insights: boolean;
  recommendations: boolean;
};

export type ReportMetadata = {
  is_preview: boolean;
  processing_mode: string;
  file_type?: string | null;
  file_size_bytes?: number | null;
  sample_row_count?: number | null;
  cache_hit?: boolean;
  job_id?: string | null;
  optimized_mode?: boolean;
  processing_strategy?: string | null;
  sample_strategy?: string | null;
  max_upload_size_bytes?: number | null;
  storage_backend?: string | null;
};

export type ReportPayload = {
  dataset_name: string;
  source_type: string;
  target_column: string | null;
  overview: {
    row_count: number;
    column_count: number;
    original_row_count: number;
    original_column_count: number;
    target_column: string | null;
    preview_rows: Record<string, string | number | boolean | null>[];
    columns: ReportColumnProfile[];
    detected_data_types: Record<string, string>;
  };
  cleaning: {
    original_shape: { rows: number; columns: number };
    cleaned_shape: { rows: number; columns: number };
    column_mapping: Record<string, string>;
    removed_all_null_columns: string[];
    empty_rows_dropped: number;
    duplicate_rows_removed: number;
    missing_values_before: number;
    missing_values_after: number;
    detected_data_types: Record<string, string>;
  };
  summary_statistics: SummaryStatistic[];
  correlations: {
    available: boolean;
    columns: string[];
    matrix: number[][];
    strongest_pairs: CorrelationPair[];
  };
  outliers: OutlierSummary[];
  trends: TrendSummary[];
  charts: ChartSpec[];
  modeling: ModelingSummary;
  insights: string[];
  recommendations: string[];
  metadata: ReportMetadata;
  sections: ReportSectionStatus;
};

export type ReportDetail = HistoryItem & {
  report: ReportPayload;
};

export type ManualEntryPayload = {
  dataset_name: string;
  columns: string[];
  rows: Array<Record<string, string | number | null>>;
  target_column?: string;
};

export type ShareLinkResponse = {
  share_token: string;
  share_url: string;
};

export type ReportRowsPage = {
  page: number;
  page_size: number;
  total_rows: number;
  total_pages: number;
  columns: string[];
  rows: Record<string, string | number | boolean | null>[];
  is_preview: boolean;
};

export type ReportSectionResponse<T = unknown> = {
  section: string;
  data: T;
};
