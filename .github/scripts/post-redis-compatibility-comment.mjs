import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const marker = "<!-- redisstreamscope-redis-compatibility-results -->";
const redisSeries = ["6.2", "7.0", "7.2", "7.4", "8.0", "8.2", "8.4", "8.6", "8.8"];
const resultDirectory = process.env.REDIS_RESULTS_DIR || "redis-results";

function readResult(series) {
  const resultPath = join(resultDirectory, `${series}.txt`);
  if (!existsSync(resultPath)) {
    return {
      editionAndVersion: `Redis Open Source ${series} series`,
      result: "NOT RUN",
    };
  }

  const [actualVersion, recordedResult] = readFileSync(resultPath, "utf8")
    .trim()
    .split(/\r?\n/, 2);
  const result = recordedResult === "PASS" ? "PASS" : "FAIL";
  return {
    editionAndVersion: `Redis Open Source ${actualVersion || `${series} series`}`,
    result,
  };
}

const rows = redisSeries.map((series) => readResult(series));
const body = [
  marker,
  "## Redis compatibility test results",
  "",
  "| Redis edition and version | Test result |",
  "| --- | --- |",
  ...rows.map(({ editionAndVersion, result }) => `| ${editionAndVersion} | ${result} |`),
  "",
  `Commit: \`${(process.env.COMMIT_SHA || "").slice(0, 12)}\``,
].join("\n");

if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${body}\n`);
}

if (process.env.COMMENT_DRY_RUN === "1") {
  process.stdout.write(`${body}\n`);
  process.exit(0);
}

const repository = process.env.REPOSITORY;
const pullRequestNumber = process.env.PULL_REQUEST_NUMBER;
if (!repository || !pullRequestNumber) {
  throw new Error("REPOSITORY and PULL_REQUEST_NUMBER are required");
}

const commentsPath = `repos/${repository}/issues/${pullRequestNumber}/comments`;
const existingCommentID = execFileSync(
  "gh",
  [
    "api",
    commentsPath,
    "--paginate",
    "--jq",
    `.[] | select(.body | startswith("${marker}")) | .id`,
  ],
  { encoding: "utf8" },
)
  .trim()
  .split(/\r?\n/, 1)[0];

if (existingCommentID) {
  execFileSync(
    "gh",
    [
      "api",
      "--method",
      "PATCH",
      `repos/${repository}/issues/comments/${existingCommentID}`,
      "--raw-field",
      `body=${body}`,
    ],
    { stdio: "inherit" },
  );
} else {
  execFileSync(
    "gh",
    [
      "api",
      "--method",
      "POST",
      commentsPath,
      "--raw-field",
      `body=${body}`,
    ],
    { stdio: "inherit" },
  );
}
