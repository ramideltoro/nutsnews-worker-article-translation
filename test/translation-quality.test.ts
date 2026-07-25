import {
  describe,
  expect,
  it
} from "vitest";

import { LocalTranslationQualityValidator } from "../src/test-doubles.js";

describe("LocalTranslationQualityValidator", () => {
  it("accepts sanitized language fixtures at the configured regression threshold", () => {
    const validator = new LocalTranslationQualityValidator();

    for (const fixture of validFixtures) {
      expect(validator.validate({
        sourceLanguage: "en",
        targetLanguage: fixture.language,
        summary: fixture.summary,
        qualityScore: 88,
        minQualityScore: 80,
        minSummaryChars: 24,
        maxSummaryChars: 420
      })).toMatchObject({
        ok: true
      });
    }
  });

  it("classifies malformed and low-quality outputs deterministically", () => {
    const validator = new LocalTranslationQualityValidator();

    expect(invalidReason(validator, "", "fr")).toBe("empty_summary");
    expect(invalidReason(validator, "Court.", "fr")).toBe("summary_too_short");
    expect(invalidReason(validator, "texte ".repeat(120), "fr")).toBe("summary_too_long");
    expect(invalidReason(validator, "Le rapport contient un caractere de remplacement \uFFFD dans le texte.", "fr")).toBe("encoding_error");
    expect(invalidReason(validator, "As an AI, I cannot translate this article for publication.", "fr")).toBe("prohibited_boilerplate");
    expect(invalidReason(validator, "The article reports a useful public-interest development with details.", "fr")).toBe("source_copy_leakage");
    expect(invalidReason(validator, "The article reports a useful public-interest development with details.", "ja")).toBe("source_copy_leakage");
    expect(invalidReason(validator, "Le rapport decrit une avancee utile avec assez de details.", "ja")).toBe("target_language_script_mismatch");
    expect(invalidReason(validator, "Le rapport decrit une avancee utile avec assez de details.", "fr", 50)).toBe("translation_quality_below_threshold");
  });

  it("does not classify French article cognates as source copy leakage", () => {
    const validator = new LocalTranslationQualityValidator();

    expect(validator.validate({
      sourceLanguage: "en",
      targetLanguage: "fr",
      summary: "L'article presente une avancee utile pour le public avec des details suffisants.",
      qualityScore: 88,
      minQualityScore: 80,
      minSummaryChars: 24,
      maxSummaryChars: 420
    })).toMatchObject({
      ok: true
    });
  });
});

const validFixtures = [
  {
    language: "fr",
    summary: "Le rapport decrit une avancee utile pour le public avec des details suffisants."
  },
  {
    language: "ja",
    summary: "この記事は、地域社会に役立つ進展を具体的に伝えています。"
  },
  {
    language: "de-CH",
    summary: "Der Bericht beschreibt eine konkrete Entwicklung mit klarem Nutzen fuer die Oeffentlichkeit."
  },
  {
    language: "de",
    summary: "Der Bericht beschreibt eine konkrete Entwicklung mit erkennbarem Nutzen fuer die Oeffentlichkeit."
  },
  {
    language: "el",
    summary: "Το άρθρο περιγράφει μια χρήσιμη εξέλιξη για το κοινό με σαφείς λεπτομέρειες."
  }
] as const;

function invalidReason(
  validator: LocalTranslationQualityValidator,
  summary: string,
  targetLanguage: string,
  qualityScore = 88
): string {
  const result = validator.validate({
    sourceLanguage: "en",
    targetLanguage,
    summary,
    qualityScore,
    minQualityScore: 80,
    minSummaryChars: 24,
    maxSummaryChars: 420
  });

  if (result.ok) {
    throw new Error("Expected invalid translation quality fixture.");
  }

  expect(result.retryable).toBe(true);

  return result.reason;
}
