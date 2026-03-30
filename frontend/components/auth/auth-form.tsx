"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";

import { useAuth } from "@/components/providers/auth-provider";

type AuthFormProps = {
  initialMode?: "login" | "signup";
};

export function AuthForm({ initialMode = "signup" }: AuthFormProps) {
  const router = useRouter();
  const { loginPublicUser, loginUser, signupUser } = useAuth();
  const [mode, setMode] = useState<"login" | "signup">(initialMode);
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setIsSubmitting(true);

    try {
      if (mode === "signup") {
        await signupUser({ full_name: fullName, email, password });
      } else {
        await loginUser({ email, password });
      }
      router.push("/dashboard");
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "Authentication failed. Please try again."
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  async function handlePublicAccess() {
    setError("");
    setIsSubmitting(true);

    try {
      await loginPublicUser();
      router.push("/dashboard");
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "Public access failed. Please try again."
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <div className="auth-card">
      <div className="section-eyebrow">Secure workspace</div>
      <h1 className="page-title">{mode === "signup" ? "Create your analytics workspace" : "Welcome back"}</h1>
      <p className="muted-copy">
        Upload CSV or Excel files, enter data manually, and let the app generate charts, insights, and model suggestions automatically.
      </p>

      <div className="toggle-row">
        <button
          type="button"
          className={mode === "signup" ? "segmented-button segmented-button--active" : "segmented-button"}
          onClick={() => setMode("signup")}
        >
          Sign up
        </button>
        <button
          type="button"
          className={mode === "login" ? "segmented-button segmented-button--active" : "segmented-button"}
          onClick={() => setMode("login")}
        >
          Log in
        </button>
      </div>

      <form className="stack" onSubmit={handleSubmit}>
        {mode === "signup" ? (
          <label className="field">
            <span>Full name</span>
            <input
              className="input"
              value={fullName}
              onChange={(event) => setFullName(event.target.value)}
              placeholder="Aisha Patel"
              required
            />
          </label>
        ) : null}

        <label className="field">
          <span>Email address</span>
          <input
            className="input"
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="you@company.com"
            required
          />
        </label>

        <label className="field">
          <span>Password</span>
          <input
            className="input"
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Minimum 8 characters"
            minLength={8}
            required
          />
        </label>

        {error ? <div className="notice notice--error">{error}</div> : null}

        <button type="submit" className="button button--primary button--full" disabled={isSubmitting}>
          {isSubmitting ? "Please wait..." : mode === "signup" ? "Create account" : "Continue"}
        </button>
      </form>

      <div className="stack" style={{ marginTop: "1rem" }}>
        <button
          type="button"
          className="button button--secondary button--full"
          onClick={handlePublicAccess}
          disabled={isSubmitting}
        >
          {isSubmitting ? "Please wait..." : "Continue as public user"}
        </button>
        <p className="muted-copy">
          Public access skips account creation and restores itself after browser or app restarts.
        </p>
      </div>
    </div>
  );
}
