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
  dataset_name: string;
  source_type: string;
  target_column: string | null;
  row_count: number;
  column_count: number;
  status: string;
  share_token: string;
  share_url: string;
  created_at: string;
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
  mean?: number;
  median?: number;
  std?: number;
  min?: number | string | null;
  max?: number | string | null;
  q1?: number;
  q3?: number;
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

