"use client";

import { useEffect, useRef, useState } from "react";

import { checkApiHealth } from "@/lib/api";

type ConnectionState = "checking" | "connected" | "offline" | "unreachable";
const RETRY_DELAY_MS = 5000;

export function ConnectionBanner() {
  const [state, setState] = useState<ConnectionState>("checking");
  const [isRetrying, setIsRetrying] = useState(false);
  const retryTimeoutRef = useRef<number | null>(null);
  const retryConnectionRef = useRef<() => Promise<void>>(async () => undefined);

  useEffect(() => {
    let cancelled = false;

    function clearRetryTimeout() {
      if (retryTimeoutRef.current !== null) {
        window.clearTimeout(retryTimeoutRef.current);
        retryTimeoutRef.current = null;
      }
    }

    async function verifyConnection(scheduleRetry = true) {
      clearRetryTimeout();

      if (!navigator.onLine) {
        if (!cancelled) {
          setState("offline");
          setIsRetrying(false);
        }
        return;
      }

      if (!cancelled) {
        setState("checking");
        setIsRetrying(true);
      }

      try {
        await checkApiHealth();
        if (!cancelled) {
          setState("connected");
        }
      } catch {
        if (!cancelled) {
          setState("unreachable");
          if (scheduleRetry && navigator.onLine) {
            retryTimeoutRef.current = window.setTimeout(() => {
              if (!cancelled) {
                void verifyConnection();
              }
            }, RETRY_DELAY_MS);
          }
        }
      } finally {
        if (!cancelled) {
          setIsRetrying(false);
        }
      }
    }

    function handleOffline() {
      clearRetryTimeout();
      setState("offline");
      setIsRetrying(false);
    }

    function handleOnline() {
      void verifyConnection();
    }

    function handleVisibilityChange() {
      if (document.visibilityState === "visible") {
        void verifyConnection(false);
      }
    }

    retryConnectionRef.current = () => verifyConnection();
    void verifyConnection();
    window.addEventListener("offline", handleOffline);
    window.addEventListener("online", handleOnline);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      cancelled = true;
      clearRetryTimeout();
      retryConnectionRef.current = async () => undefined;
      window.removeEventListener("offline", handleOffline);
      window.removeEventListener("online", handleOnline);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  async function handleRetryConnection() {
    if (!navigator.onLine) {
      setState("offline");
      setIsRetrying(false);
      return;
    }

    await retryConnectionRef.current();
  }

  if (state === "connected") {
    return null;
  }

  const message =
    state === "offline"
      ? "You appear to be offline. Please check your internet connection and try again."
      : state === "checking"
        ? "Connecting to the analytics backend. Please wait a moment."
        : "Backend is waking up or temporarily unreachable. Retrying automatically every few seconds.";

  return (
    <div className="connection-banner">
      <div className={`shell notice ${state === "checking" ? "notice--info" : "notice--warning"}`}>
        <div className="connection-banner__content">
          <span>{message}</span>
          <button
            type="button"
            className="button button--ghost connection-banner__button"
            onClick={() => void handleRetryConnection()}
            disabled={isRetrying}
          >
            {isRetrying ? "Checking..." : "Retry connection"}
          </button>
        </div>
      </div>
    </div>
  );
}
