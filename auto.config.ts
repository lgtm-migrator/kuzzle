import { AutoRc } from "auto";

import { INpmConfig } from "@auto-it/npm";

const npmOptions: INpmConfig = {
  exact: true,
  canaryScope: "@auto-canary",
};

/** Auto configuration */
export default function rc() {
  return {
    plugins: [
      "released",
      ["npm", npmOptions],
    ],
    versionBranches: "major-",
    baseBranch: "master",
    prereleaseBranches: ["1-dev", "2-dev"],
    labels: [
      {
        "name": "changelog:breaking-changes",
        "changelogTitle": "💥 Breaking Changes",
        "description": " Increase the major version number",
        "releaseType": "major",
        "color": "#C5000B",
        "overwrite": true
      },
      {
        "name": "changelog:new-features",
        "changelogTitle": "🚀 New Features",
        "description": " Increase the minor version number",
        "releaseType": "minor",
        "color": "#F1A60E",
        "overwrite": true
      },
      {
        "name": "changelog:bug-fixes",
        "changelogTitle": "🐛 Bug Fixes",
        "description": " z",
        "releaseType": "patch",
        "color": "#870048",
        "overwrite": true
      },
      {
        "name": "release",
        "description": "Trigger the release process",
        "releaseType": "release",
        "color": "#007f70",
        "overwrite": true
      },
      {
        "name": "changelog:exclude",
        "changelogTitle": "🏠 Internal",
        "description": "Internal changes only",
        "releaseType": "none",
        "color": "#696969",
        "overwrite": true
      },
    ]
  };
}