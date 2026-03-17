/**
 * chat-bridge.js — postMessage bridge between Quarto pages and chat iframe
 *
 * Handles:
 * - iframe → parent: category-clicked → navigate to category page
 * - parent → iframe: ask-about → inject a question into chat
 * - Theme sync: detect Quarto dark/light and forward to iframe
 */

// ── Sidebar toggle ──────────────────────────────────────────────────────
function toggleChatSidebar() {
  const sidebar = document.getElementById("chat-sidebar");
  if (sidebar) sidebar.classList.toggle("collapsed");
}

// ── "Ask about this" from category pages ────────────────────────────────
function askChat(categoryName) {
  const sidebar = document.getElementById("chat-sidebar");
  if (sidebar && sidebar.classList.contains("collapsed")) {
    sidebar.classList.remove("collapsed");
  }
  const iframe = document.getElementById("chat-iframe");
  if (iframe && iframe.contentWindow) {
    iframe.contentWindow.postMessage({ type: "ask-about", name: categoryName }, "*");
  }
}

// ── Listen for messages from chat iframe ────────────────────────────────
window.addEventListener("message", (e) => {
  if (!e.data || typeof e.data !== "object") return;

  if (e.data.type === "category-clicked") {
    // Navigate to the category page
    const name = e.data.name;
    // Convert name to slug: lowercase, spaces→hyphens, strip special chars
    const slug = name.toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/[\s]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
    // Navigate relative to site root
    const base = document.querySelector("base")?.href || window.location.origin;
    window.location.href = `/categories/${slug}.html`;
  }
});

// ── Theme sync ──────────────────────────────────────────────────────────
function syncThemeToIframe() {
  const iframe = document.getElementById("chat-iframe");
  if (!iframe || !iframe.contentWindow) return;
  const isLight = document.body.classList.contains("quarto-light") ||
    document.documentElement.getAttribute("data-bs-theme") === "light";
  iframe.contentWindow.postMessage({ type: "set-theme", light: isLight }, "*");
}

// Watch for Quarto theme changes
const themeObserver = new MutationObserver(syncThemeToIframe);
themeObserver.observe(document.body, { attributes: true, attributeFilter: ["class"] });
themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["data-bs-theme"] });

// Sync on iframe load
const chatIframe = document.getElementById("chat-iframe");
if (chatIframe) {
  chatIframe.addEventListener("load", syncThemeToIframe);
}
