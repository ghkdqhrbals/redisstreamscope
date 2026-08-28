const copyButtons = document.querySelectorAll("[data-copy]");
const showcaseTabs = document.querySelectorAll("[data-showcase-tab]");
const showcasePanels = document.querySelectorAll("[data-showcase-panel]");

const copyText = async (value) => {
  try {
    await navigator.clipboard.writeText(value);
    return;
  } catch {
    const input = document.createElement("textarea");
    input.value = value;
    input.setAttribute("readonly", "");
    input.style.position = "fixed";
    input.style.left = "-9999px";
    document.body.append(input);
    input.select();
    const copied = document.execCommand("copy");
    input.remove();
    if (!copied) throw new Error("Clipboard access is unavailable");
  }
};

copyButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await copyText(button.dataset.copy);
      button.dataset.state = "copied";
      button.textContent = "Copied";

      window.setTimeout(() => {
        button.dataset.state = "";
        button.textContent = "Copy";
      }, 3000);
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
