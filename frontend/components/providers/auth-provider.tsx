"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren
} from "react";

import { ApiError, getCurrentUser, login, loginPublicUser as requestPublicLogin, signup } from "@/lib/api";
import { clearCachedAuthState, readCachedAuthState, writeCachedAuthState } from "@/lib/client-cache";
import type { AuthMode, LoginPayload, SignupPayload, User } from "@/lib/types";

type AuthContextValue = {
  user: User | null;
  token: string | null;
  authMode: AuthMode | null;
  isLoading: boolean;
  loginUser: (payload: LoginPayload) => Promise<void>;
  signupUser: (payload: SignupPayload) => Promise<void>;
  loginPublicUser: () => Promise<void>;
  logoutUser: () => void;
};

const PUBLIC_AUTH_MODE: AuthMode = "public";
const ACCOUNT_AUTH_MODE: AuthMode = "account";

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export function AuthProvider({ children }: PropsWithChildren) {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authMode, setAuthMode] = useState<AuthMode | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  function clearAuthState() {
    setToken(null);
    setUser(null);
    setAuthMode(null);
    clearCachedAuthState();
  }

  function persistAuthState(nextToken: string, nextMode: AuthMode, nextUser: User | null) {
    setToken(nextToken);
    setAuthMode(nextMode);
    setUser(nextUser);
    writeCachedAuthState({ token: nextToken, mode: nextMode, user: nextUser });
  }

  async function restorePublicUser() {
    const response = await requestPublicLogin();
    persistAuthState(response.access_token, PUBLIC_AUTH_MODE, response.user);
  }

  useEffect(() => {
    const cachedState = readCachedAuthState();
    if (!cachedState) {
      setIsLoading(false);
      return;
    }

    try {
      const storedMode = cachedState.mode === PUBLIC_AUTH_MODE ? PUBLIC_AUTH_MODE : ACCOUNT_AUTH_MODE;
      if (!cachedState.token) {
        if (storedMode === PUBLIC_AUTH_MODE) {
          restorePublicUser()
            .catch(() => {
              clearAuthState();
            })
            .finally(() => setIsLoading(false));
          return;
        }

        clearAuthState();
        setIsLoading(false);
        return;
      }

      setToken(cachedState.token);
      setAuthMode(storedMode);
      setUser(cachedState.user ?? null);
      setIsLoading(false);

      getCurrentUser(cachedState.token)
        .then((currentUser) => {
          setUser(currentUser);
          writeCachedAuthState({
            token: cachedState.token!,
            mode: storedMode,
            user: currentUser
          });
        })
        .catch(async (error) => {
          if (!(error instanceof ApiError) || ![401, 403].includes(error.status ?? 0)) {
            return;
          }

          if (storedMode === PUBLIC_AUTH_MODE) {
            try {
              await restorePublicUser();
            } catch {
              clearAuthState();
            }
            return;
          }

          clearAuthState();
        });
    } catch {
      clearAuthState();
      setIsLoading(false);
    }
  }, []);

  async function loginUser(payload: LoginPayload) {
    const response = await login(payload);
    persistAuthState(response.access_token, ACCOUNT_AUTH_MODE, response.user);
  }

  async function signupUser(payload: SignupPayload) {
    const response = await signup(payload);
    persistAuthState(response.access_token, ACCOUNT_AUTH_MODE, response.user);
  }

  async function loginPublicUser() {
    await restorePublicUser();
  }

  function logoutUser() {
    clearAuthState();
  }

  const value = useMemo(
    () => ({ user, token, authMode, isLoading, loginUser, signupUser, loginPublicUser, logoutUser }),
    [user, token, authMode, isLoading]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used inside an AuthProvider.");
  }
  return context;
}
