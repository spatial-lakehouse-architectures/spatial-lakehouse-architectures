(function () {
  "use strict";

  // -------- Mobile nav --------
  const navToggle = document.querySelector(".nav-toggle");
  const nav = document.getElementById("primary-nav");
  if (navToggle && nav) {
    navToggle.addEventListener("click", () => {
      const open = navToggle.getAttribute("aria-expanded") === "true";
      navToggle.setAttribute("aria-expanded", String(!open));
      nav.classList.toggle("is-open", !open);
    });
    // Collapse on link click
    nav.addEventListener("click", (e) => {
      if (e.target.closest("a") && window.matchMedia("(max-width: 960px)").matches) {
        navToggle.setAttribute("aria-expanded", "false");
        nav.classList.remove("is-open");
      }
    });
  }

  // -------- Copy buttons on code blocks --------
  document.querySelectorAll(".code-block").forEach((block) => {
    const btn = block.querySelector(".code-block__copy");
    const pre = block.querySelector("pre");
    if (!btn || !pre) return;
    btn.addEventListener("click", async () => {
      const text = pre.innerText.replace(/ /g, " ");
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
        } else {
          const ta = document.createElement("textarea");
          ta.value = text;
          ta.style.position = "fixed";
          ta.style.left = "-9999px";
          document.body.appendChild(ta);
          ta.select();
          document.execCommand("copy");
          ta.remove();
        }
        const original = btn.textContent;
        btn.textContent = "Copied";
        btn.classList.add("is-copied");
        setTimeout(() => {
          btn.textContent = original;
          btn.classList.remove("is-copied");
        }, 1500);
      } catch (e) {
        btn.textContent = "Press Ctrl-C";
      }
    });
  });

  // -------- Task list checkbox persistence --------
  const storageKey = "tasks:" + location.pathname;
  const stored = (() => {
    try { return JSON.parse(localStorage.getItem(storageKey) || "{}"); }
    catch (_) { return {}; }
  })();
  const checkboxes = document.querySelectorAll(".markdown-body .task-list-checkbox");
  checkboxes.forEach((cb, idx) => {
    if (Object.prototype.hasOwnProperty.call(stored, idx)) {
      cb.checked = !!stored[idx];
    }
    const li = cb.closest(".task-list-item");
    if (li) li.classList.toggle("is-checked", cb.checked);
    cb.addEventListener("change", () => {
      stored[idx] = cb.checked;
      try { localStorage.setItem(storageKey, JSON.stringify(stored)); } catch (_) {}
      if (li) li.classList.toggle("is-checked", cb.checked);
    });
  });

  // -------- Mermaid runtime --------
  const mermaidNodes = document.querySelectorAll("pre.mermaid");
  if (mermaidNodes.length > 0) {
    const s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js";
    s.onload = () => {
      if (!window.mermaid) return;
      window.mermaid.initialize({
        startOnLoad: false,
        theme: "base",
        themeVariables: {
          primaryColor: "#d8eff2",
          primaryTextColor: "#1d2a33",
          primaryBorderColor: "#0e6e7d",
          lineColor: "#0e6e7d",
          secondaryColor: "#f1c560",
          tertiaryColor: "#7ec48b",
          fontFamily: 'Inter, system-ui, sans-serif',
        },
      });
      window.mermaid.run({ nodes: mermaidNodes });
    };
    document.head.appendChild(s);
  }

  // -------- Service worker --------
  if ("serviceWorker" in navigator && (location.protocol === "https:" || location.hostname === "localhost")) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("/sw.js").catch(() => {});
    });
  }
})();
