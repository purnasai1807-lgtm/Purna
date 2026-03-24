import type { Metadata } from "next";
import { Manrope, Space_Grotesk } from "next/font/google";
import type { ReactNode } from "react";

import { SiteHeader } from "@/components/layout/site-header";
import { AuthProvider } from "@/components/providers/auth-provider";
import { ConnectionBanner } from "@/components/system/connection-banner";

import "./globals.css";

const bodyFont = Manrope({
  subsets: ["latin"],
  variable: "--font-body"
});

const headingFont = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-heading"
});

export const metadata: Metadata = {
  title: "Auto Analytics AI",
  description: "Production-ready AI analytics workspace for uploads, manual entry, charts, insights, and model suggestions."
};

export default function RootLayout({
  children
}: Readonly<{
  children: ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${bodyFont.variable} ${headingFont.variable}`}>
        <AuthProvider>
          <SiteHeader />
          <ConnectionBanner />
          {children}
        </AuthProvider>
      </body>
    </html>
  );
}
