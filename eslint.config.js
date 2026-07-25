import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: [
      "coverage/**",
      "dist/**",
      "node_modules/**",
      "*.tgz",
      "*.sbom.json"
    ]
  },
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.eslint.json",
        tsconfigRootDir: import.meta.dirname
      }
    },
    rules: {
      "@typescript-eslint/consistent-type-definitions": [
        "error",
        "interface"
      ],
      "@typescript-eslint/no-magic-numbers": "off",
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/no-empty-function": "off"
    }
  },
  {
    files: [
      "eslint.config.js"
    ],
    extends: [
      tseslint.configs.disableTypeChecked
    ],
    languageOptions: {
      globals: {
        console: "readonly",
        process: "readonly",
        URL: "readonly"
      },
      sourceType: "module"
    }
  },
  {
    files: [
      "test/**/*.ts"
    ],
    rules: {
      "@typescript-eslint/no-magic-numbers": "off"
    }
  }
);
