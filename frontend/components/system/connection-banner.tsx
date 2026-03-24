"use client";

import { useEffect, useState } from "react";

import { checkApiHealth } from "@/lib/api";

type ConnectionState = "checking" | "connected" | "offline" | "unreachable";

export function ConnectionBanner() {
  const [state, setState] = useState<ConnectionState>("checking");
  const [isRetrying, setIsRetrying] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function verifyConnection() {
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
        }
      } finally {
        if (!cancelled) {
          setIsRetrying(false);
        }
      }
    }

    function handleOffline() {
      setState("offline");
      setIsRetrying(false);
    }

    function handleOnline() {
      void verifyConnection();
    }

    function handleVisibilityChange() {
      if (document.visibilityState === "visible") {
        void verifyConnection();
      }
    }

    void verifyConnection();
    window.addEventListener("offline", handleOffline);
    window.addEventListener("online", handleOnline);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      cancelled = true;
      window.removeEventListener("offline", handleOffline);
      window.removeEventListener("online", handleOnline);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  async function handleRetryConnection() {
    setIsRetrying(true);
    setState(navigator.onLine ? "checking" : "offline");

    if (!navigator.onLine) {
      setIsRetrying(false);
      return;
    }

    try {
      await checkApiHealth();
      setState("connected");
    } catch {
      setState("unreachable");
    } finally {
      setIsRetrying(false);
    }
  }

  if (state === "connected") {
    return null;
  }

  const message =
    state === "offline"
      ? "You appear to be offline. Please check your internet connection and try again."
      : state === "checking"
        ? "Connecting to the analytics service. Please wait a moment."
        : "The analytics service is waking up or temporarily unreachable. Please wait a few seconds and retry.";

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
