"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

import { AuthForm } from "@/components/auth/auth-form";
import { useAuth } from "@/components/providers/auth-provider";
import { LoadingSpinner } from "@/components/ui/loading-spinner";

export default function AuthPage() {
  const router = useRouter();
  const { user, isLoading } = useAuth();

  useEffect(() => {
    if (!isLoading && user) {
      router.replace("/dashboard");
    }
  }, [isLoading, router, user]);

  if (isLoading) {
    return (
      <main className="page-shell page-shell--centered">
        <LoadingSpinner label="Checking your workspace..." />
      </main>
    );
  }

  return (
    <main className="page-shell">
      <div className="shell auth-layout">
        <AuthForm />
        <aside className="auth-side-panel">
          <div className="section-eyebrow">What you get</div>
          <h2>One workflow from raw file to stakeholder-ready report.</h2>
          <ul className="bullet-list">
            <li>Account-based history so users can revisit old analyses.</li>
            <li>Automatic chart generation optimized for both mobile and desktop dashboards.</li>
            <li>Baseline machine learning suggestions with evaluation metrics and recommended next steps.</li>
            <li>Shareable URLs and downloadable PDF reports for fast collaboration.</li>
          </ul>
        </aside>
      </div>
    </main>
  );
}

