import { createContext, useContext } from "react";

type Variables = Record<string, string | number>;

function interpolate(message: string, variables?: Variables) {
  if (!variables) return message;
  return Object.entries(variables).reduce(
    (result, [key, value]) => result.replaceAll(`{${key}}`, String(value)),
    message,
  );
}

type I18nValue = {
  locale: "en-US";
  t: (message: string, variables?: Variables) => string;
};

const english = {
  locale: "en-US",
  t: interpolate,
} satisfies I18nValue;

const I18nContext = createContext<I18nValue | null>(null);

export function LanguageProvider({ children }: { children: React.ReactNode }) {
  return <I18nContext.Provider value={english}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  const value = useContext(I18nContext);
  if (!value) throw new Error("useI18n must be used inside LanguageProvider");
  return value;
}
