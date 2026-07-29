const languageButtons = document.querySelectorAll("[data-language]");
const translatedElements = document.querySelectorAll("[data-en][data-ko]");
const translatedImages = document.querySelectorAll("[data-alt-en][data-alt-ko]");
const copyButtons = document.querySelectorAll("[data-copy]");

const savedLanguage = localStorage.getItem("streamscope-docs-language");
const initialLanguage = savedLanguage === "ko" ? "ko" : "en";
let activeLanguage = initialLanguage;

function setLanguage(language) {
  activeLanguage = language === "ko" ? "ko" : "en";
  document.documentElement.lang = activeLanguage;
  document.title =
    activeLanguage === "ko"
      ? "StreamScope — Redis Streams 콘솔"
      : "StreamScope — Redis Streams Console";

  translatedElements.forEach((element) => {
    element.textContent = element.dataset[activeLanguage];
  });

  translatedImages.forEach((image) => {
    image.alt = image.dataset[`alt${activeLanguage === "ko" ? "Ko" : "En"}`];
  });

  languageButtons.forEach((button) => {
    button.setAttribute(
      "aria-pressed",
      String(button.dataset.language === activeLanguage),
    );
  });

  localStorage.setItem("streamscope-docs-language", activeLanguage);
}

languageButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setLanguage(button.dataset.language);
  });
});

copyButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      button.dataset.state = "copied";
      button.textContent =
        activeLanguage === "ko"
          ? button.dataset.copiedKo
          : button.dataset.copiedEn;

      window.setTimeout(() => {
        button.dataset.state = "";
        button.textContent = button.dataset[activeLanguage];
      }, 1600);
    } catch {
      button.textContent = button.dataset[activeLanguage];
    }
  });
});

setLanguage(initialLanguage);
