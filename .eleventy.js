const fs = require("fs");
const path = require("path");
const markdownIt = require("markdown-it");
const markdownItAnchor = require("markdown-it-anchor");
const markdownItAttrs = require("markdown-it-attrs");
const markdownItKatex = require("markdown-it-katex");
const hljs = require("highlight.js");
const eleventyNavigation = require("@11ty/eleventy-navigation");

const SITE_URL = "https://www.spatial-lakehouse-architectures.org";

function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-");
}

function highlight(str, lang) {
  const language = (lang || "").trim().toLowerCase();
  if (language === "mermaid") {
    return `<pre class="mermaid">${escapeHtml(str)}</pre>`;
  }
  if (language && hljs.getLanguage(language)) {
    try {
      const out = hljs.highlight(str, { language, ignoreIllegals: true }).value;
      return wrapCodeBlock(out, language);
    } catch (e) {}
  }
  return wrapCodeBlock(escapeHtml(str), language || "text");
}

function escapeHtml(s) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function wrapCodeBlock(htmlContent, lang) {
  const label = lang === "text" ? "" : lang;
  return (
    `<div class="code-block" data-lang="${escapeHtml(label)}">` +
    `<div class="code-block__toolbar">` +
    (label ? `<span class="code-block__lang">${escapeHtml(label)}</span>` : `<span class="code-block__lang"></span>`) +
    `<button type="button" class="code-block__copy" aria-label="Copy code">Copy</button>` +
    `</div>` +
    `<pre class="hljs" tabindex="0" role="group" aria-label="${escapeHtml(label || "code")} code sample"><code class="language-${escapeHtml(lang)}">${htmlContent}</code></pre>` +
    `</div>`
  );
}

module.exports = function (eleventyConfig) {
  eleventyConfig.addPlugin(eleventyNavigation);

  // Passthrough copy
  eleventyConfig.addPassthroughCopy({ "src/assets": "assets" });
  eleventyConfig.addPassthroughCopy({ "src/manifest.webmanifest": "manifest.webmanifest" });
  eleventyConfig.addPassthroughCopy({ "src/sw.js": "sw.js" });
  eleventyConfig.addPassthroughCopy({ "src/robots.txt": "robots.txt" });
  eleventyConfig.addPassthroughCopy({ "src/fcea9306acc01a8dffd3f4d4e11a3b1d.txt": "fcea9306acc01a8dffd3f4d4e11a3b1d.txt" });

  // Markdown configuration
  const md = markdownIt({
    html: true,
    linkify: true,
    typographer: true,
    breaks: false,
    highlight,
  })
    .use(markdownItAnchor, {
      level: [2, 3, 4],
      permalink: markdownItAnchor.permalink.headerLink(),
      slugify,
    })
    .use(markdownItAttrs)
    .use(markdownItKatex, { throwOnError: false, errorColor: "#c0392b" });

  // ----- Custom: interactive task-list checkboxes -----
  // Renders `[ ]`/`[x]` list items as <input type="checkbox"> with a class
  // that the stylesheet uses to hide the bullet and apply line-through.
  md.core.ruler.after("inline", "task-lists", (state) => {
    const tokens = state.tokens;
    for (let i = 0; i < tokens.length; i++) {
      if (tokens[i].type !== "inline") continue;
      const parent = tokens[i - 1];
      const grand = tokens[i - 2];
      if (!parent || parent.type !== "paragraph_open") continue;
      if (!grand || grand.type !== "list_item_open") continue;
      const inline = tokens[i];
      if (!inline.children || inline.children.length < 1) continue;
      const first = inline.children[0];
      if (first.type !== "text") continue;
      const m = first.content.match(/^\[([ xX])\]\s+/);
      if (!m) continue;
      const checked = m[1].toLowerCase() === "x";
      first.content = first.content.slice(m[0].length);
      const labelText = (first.content || "checklist item").trim().slice(0, 80)
        .replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        || "checklist item";
      const cb = new state.Token("html_inline", "", 0);
      cb.content = `<input type="checkbox" class="task-list-checkbox" disabled aria-label="${labelText}"${checked ? " checked" : ""}> `;
      inline.children.unshift(cb);
      // tag list item & its parent <ul> for CSS targeting
      grand.attrJoin("class", "task-list-item");
      // find enclosing bullet list (only tag once)
      for (let j = i - 2; j >= 0; j--) {
        if (tokens[j].type === "bullet_list_open") {
          const cls = tokens[j].attrGet("class") || "";
          if (!cls.split(/\s+/).includes("task-list")) {
            tokens[j].attrJoin("class", "task-list");
          }
          break;
        }
      }
    }
  });

  // ----- Custom: wrap tables in a scrollable container -----
  const defaultTableOpen = md.renderer.rules.table_open || function (t, i, o, e, self) { return self.renderToken(t, i, o); };
  const defaultTableClose = md.renderer.rules.table_close || function (t, i, o, e, self) { return self.renderToken(t, i, o); };
  md.renderer.rules.table_open = function (...args) {
    return `<div class="table-wrap" tabindex="0">` + defaultTableOpen(...args);
  };
  md.renderer.rules.table_close = function (...args) {
    return defaultTableClose(...args) + `</div>`;
  };

  // ----- Custom: fenced code blocks -----
  // Our `highlight` returns a full `.code-block` wrapper (not starting with `<pre`),
  // so markdown-it's default fence renderer would double-wrap it in an extra
  // `<pre><code>` that lacks `overflow-x:auto` and overflows the viewport. Render
  // the fence output directly instead.
  md.renderer.rules.fence = function (tokens, idx) {
    const token = tokens[idx];
    const info = token.info ? md.utils.unescapeAll(token.info).trim() : "";
    const lang = info.split(/\s+/g)[0] || "";
    return highlight(token.content, lang) + "\n";
  };

  eleventyConfig.setLibrary("md", md);

  // ----- Filters -----
  eleventyConfig.addFilter("absoluteUrl", (url) => {
    if (!url) return SITE_URL;
    if (url.startsWith("http")) return url;
    return SITE_URL.replace(/\/$/, "") + (url.startsWith("/") ? url : "/" + url);
  });

  eleventyConfig.addFilter("dateIso", (d) => new Date(d || Date.now()).toISOString());
  eleventyConfig.addFilter("dateYMD", (d) => new Date(d || Date.now()).toISOString().slice(0, 10));

  eleventyConfig.addFilter("breadcrumb", (url) => {
    if (!url || url === "/") return [];
    const parts = url.replace(/^\/+|\/+$/g, "").split("/");
    const crumbs = [];
    let path = "";
    for (const p of parts) {
      path += "/" + p;
      crumbs.push({ url: path + "/", slug: p });
    }
    return crumbs;
  });

  eleventyConfig.addFilter("titleFromSlug", (slug) => {
    return String(slug)
      .replace(/-/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());
  });

  eleventyConfig.addFilter("childrenOf", (collection, parentUrl) => {
    if (!Array.isArray(collection) || !parentUrl) return [];
    return collection.filter((item) => {
      if (!item.url || item.url === parentUrl) return false;
      if (!item.url.startsWith(parentUrl)) return false;
      const rest = item.url.slice(parentUrl.length).replace(/\/$/, "");
      return rest.length > 0 && !rest.includes("/");
    });
  });

  eleventyConfig.addFilter("topicsOf", (collection, pillarSlug) => {
    if (!Array.isArray(collection) || !pillarSlug) return [];
    const hub = "/" + pillarSlug + "/";
    return collection.filter((item) => {
      if (!item.url || !item.url.startsWith(hub) || item.url === hub) return false;
      const rest = item.url.slice(hub.length).replace(/\/$/, "");
      return rest.length > 0 && !rest.includes("/");
    });
  });

  // Deepest "leaf" guides (3 URL segments) — the strongest deep-dive pages.
  eleventyConfig.addFilter("deepGuides", (collection, limit) => {
    if (!Array.isArray(collection)) return [];
    const guides = collection.filter((item) => {
      if (!item.url) return false;
      const parts = item.url.replace(/^\/+|\/+$/g, "").split("/").filter(Boolean);
      return parts.length === 3;
    });
    return typeof limit === "number" ? guides.slice(0, limit) : guides;
  });

  eleventyConfig.addFilter("pillarBySlug", (pillars, slug) => {
    if (!Array.isArray(pillars)) return null;
    return pillars.find((p) => p.slug === slug) || null;
  });

  eleventyConfig.addFilter("siblingsOf", (collection, currentUrl) => {
    if (!Array.isArray(collection) || !currentUrl || currentUrl === "/") return [];
    const trimmed = currentUrl.replace(/\/$/, "");
    const parent = trimmed.slice(0, trimmed.lastIndexOf("/") + 1);
    if (!parent || parent === "/") return [];
    return collection.filter((item) => {
      if (!item.url || item.url === currentUrl || item.url === parent) return false;
      if (!item.url.startsWith(parent)) return false;
      const rest = item.url.slice(parent.length).replace(/\/$/, "");
      return rest.length > 0 && !rest.includes("/");
    });
  });

  // ----- Collections -----
  eleventyConfig.addCollection("allContent", (api) =>
    api.getAll().filter((item) => item.data.layout === "content.njk")
  );

  eleventyConfig.addCollection("pillars", (api) =>
    api.getAll().filter((item) => item.data.pillar && item.data.isPillarHub)
  );

  return {
    dir: {
      input: "src",
      includes: "_includes",
      data: "_data",
      output: "_site",
    },
    templateFormats: ["njk", "md", "html", "11ty.js"],
    markdownTemplateEngine: "njk",
    htmlTemplateEngine: "njk",
    dataTemplateEngine: "njk",
  };
};
