export function formatDate(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(new Date(value));
}

export function formatLabel(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function formatMetric(value: number | string | null | undefined): string {
  if (value === null || value === undefined) {
    return "N/A";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? value.toString() : value.toFixed(4);
  }
  return value;
}

export async function copyToClipboard(value: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(value);
    return true;
  } catch {
    return false;
  }
}

export function buildShareUrl(shareToken: string, fallbackUrl: string): string {
  if (typeof window === "undefined") {
    return fallbackUrl;
  }

  return `${window.location.origin}/share/${shareToken}`;
}
