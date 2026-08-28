const copyButtons = document.querySelectorAll("[data-copy]");
const showcaseTabs = document.querySelectorAll("[data-showcase-tab]");
const showcasePanels = document.querySelectorAll("[data-showcase-panel]");

copyButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      button.dataset.state = "copied";
      button.textContent = "Copied";

      window.setTimeout(() => {
        button.dataset.state = "";
        button.textContent = "Copy";
      }, 1600);
    } catch {
      button.textContent = "Copy";
    }
  });
});

showcaseTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    const selectedShowcase = tab.dataset.showcaseTab;

    showcaseTabs.forEach((candidate) => {
      candidate.setAttribute(
        "aria-selected",
        String(candidate === tab),
      );
    });

    showcasePanels.forEach((panel) => {
      panel.hidden = panel.dataset.showcasePanel !== selectedShowcase;
    });
  });
});
