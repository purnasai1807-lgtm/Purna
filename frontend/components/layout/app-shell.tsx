"use client";

import type { ReactNode } from "react";

import { SiteHeader } from "@/components/layout/site-header";
import { AuthProvider } from "@/components/providers/auth-provider";
import { ConnectionBanner } from "@/components/system/connection-banner";

type AppShellProps = {
  children: ReactNode;
};

export function AppShell({ children }: AppShellProps) {
  return (
    <AuthProvider>
      <SiteHeader />
      <ConnectionBanner />
      {children}
    </AuthProvider>
  );
}
