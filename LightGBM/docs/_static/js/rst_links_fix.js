$(function() {
    $('a[href^="./"][href*=".rst"]').attr('href', (i, val) => { return val.replace('.rst', '.html'); });  /* Replace '.rst' with '.html' in all internal links like './[Something].rst[#anchor]' */
    $('.wy-nav-content').each(function () { this.style.setProperty('max-width', 'none', 'important'); });
});
