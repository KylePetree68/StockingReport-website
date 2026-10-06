// Builds /tailwind.css (replaces the in-browser Play CDN). The workflow
// rebuilds it after regenerating pages; locally:
//   npx tailwindcss@3.4.19 -i tailwind.input.css -o tailwind.css --minify
module.exports = {
  content: [
    './*.html',
    './scraper.py',
    './public/waters/*.html',
  ],
};
