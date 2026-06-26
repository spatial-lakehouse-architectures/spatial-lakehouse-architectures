const fs = require("fs");

module.exports = {
  layout: "layouts/content.njk",
  permalink: (data) => {
    const stem = data.page.filePathStem
      .replace(/^\/?content\//, "/")
      .replace(/\/index$/, "");
    const url = stem === "" ? "/" : stem + "/";
    return url + "index.html";
  },
  eleventyComputed: {
    title: (data) => {
      if (data.title) return data.title;
      try {
        const src = fs.readFileSync(data.page.inputPath, "utf8");
        const m = src.match(/^#\s+(.+?)\s*$/m);
        return m ? m[1].trim() : "";
      } catch (e) {
        return "";
      }
    },
    description: (data) => {
      if (data.description) return data.description;
      try {
        const src = fs.readFileSync(data.page.inputPath, "utf8");
        const stripped = src.replace(/^---[\s\S]*?---\s*/, "").replace(/^#[^\n]*\n+/, "");
        const para = stripped.split(/\n\s*\n/).find((p) => p.trim() && !p.startsWith("#") && !p.startsWith("```"));
        if (!para) return "";
        const clean = para
          .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
          .replace(/`([^`]+)`/g, "$1")
          .replace(/\s+/g, " ")
          .trim();
        return clean.length > 160 ? clean.slice(0, 157).trimEnd() + "…" : clean;
      } catch (e) {
        return "";
      }
    },
    pillarSlug: (data) => {
      const url = (data.page && data.page.url) || "";
      const parts = url.replace(/^\/+|\/+$/g, "").split("/");
      return parts[0] || "";
    },
    isPillarHub: (data) => {
      const url = (data.page && data.page.url) || "";
      const parts = url.replace(/^\/+|\/+$/g, "").split("/").filter(Boolean);
      return parts.length === 1;
    },
    pillar: (data) => {
      const url = (data.page && data.page.url) || "";
      const slug = url.replace(/^\/+|\/+$/g, "").split("/")[0] || "";
      return slug;
    },
    seoTitle: (data) => {
      // Respect explicit seoTitle set in frontmatter via _seoTitle key
      if (data._seoTitle) return data._seoTitle;
      const url = (data.page && data.page.url) || "";
      const parts = url.replace(/^\/+|\/+$/g, "").split("/").filter(Boolean);
      if (parts.length === 1 && data.site && Array.isArray(data.site.pillars)) {
        const match = data.site.pillars.find((p) => p.slug === parts[0]);
        if (match) {
          // Strip &amp; and other HTML entities for proper length
          const title = match.title.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">");
          return title;
        }
      }
      const t = data.title || "";
      if (!t) return "";
      const beforeColon = t.split(":")[0].trim();
      if (beforeColon.length >= 20 && beforeColon.length < t.length) return beforeColon;
      // Truncate if still too long (keep under 46 chars so total stays ≤65 with suffix)
      if (t.length > 46) return t.slice(0, 43).trimEnd() + "…";
      return t;
    },
  },
  tags: ["content"],
};
