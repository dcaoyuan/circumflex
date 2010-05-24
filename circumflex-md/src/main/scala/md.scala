package ru.circumflex.md

import ru.circumflex.core._
import java.util.regex._
import java.util.Random
import java.lang.StringBuilder

// # The Markdown Processor

/**
 * This utility converts a plain text written in [Markdown][1] into HTML fragment.
 * The typical usage is:
 *
 *     val md = Markdown(myMarkdownText)
 *
 *  [1]: http://daringfireball.net/projects/markdown/syntax "Markdown Syntax"
 */
object Markdown {

  // ## SmartyPants chars

  val leftQuote = Circumflex.get("md.leftQuote") match {
    case Some(s: String) => s
    case _ => "&ldquo;"
  }
  val rightQuote = Circumflex.get("md.rightQuote") match {
    case Some(s: String) => s
    case _ => "&rdquo;"
  }
  val dash = Circumflex.get("md.dash") match {
    case Some(s: String) => s
    case _ => "&mdash;"
  }
  val copy = Circumflex.get("md.copy") match {
    case Some(s: String) => s
    case _ => "&copy;"
  }
  val reg = Circumflex.get("md.reg") match {
    case Some(s: String) => s
    case _ => "&reg;"
  }
  val trademark = Circumflex.get("md.trademark") match {
    case Some(s: String) => s
    case _ => "&trade;"
  }
  val ellipsis = Circumflex.get("md.ellipsis") match {
    case Some(s: String) => s
    case _ => "&hellip;"
  }

  // ## Commons

  val keySize = 20
  val chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val rnd = new Random
  val blockTags = "p" :: "div" :: "h1" :: "h2" :: "h3" :: "h4" :: "h5" :: "h6" ::
      "blockquote" :: "pre" :: "table" :: "dl" :: "ol" :: "ul" :: "script" ::
      "noscript" :: "form" :: "fieldset" :: "iframe" :: "math" :: "ins" :: "del" ::
      "article" :: "aside" :: "footer" :: "header" :: "hgroup" :: "nav" :: "section" ::
      "figure" :: "video" :: "audio" :: "embed" :: "canvas" :: "address" :: "details" ::
      "object" ::  Nil
  val htmlNameTokenExpr = "[a-z_:][a-z0-9\\-_:.]*"

  // ## Regex patterns

  // We use precompile several regular expressions that are used for typical
  // transformations.

  // Outdent
  val rOutdent = Pattern.compile("^ {1,4}", Pattern.MULTILINE)
  // Standardize line endings
  val rLineEnds = Pattern.compile("\\r\\n|\\r")
  // Strip out whitespaces in blank lines
  val rBlankLines = Pattern.compile("^ +$", Pattern.MULTILINE)
  // Tabs
  val rTabs = Pattern.compile("\\t")
  // Trailing whitespace
  val rTrailingWS = Pattern.compile("\\s+$")
  // Start of inline HTML block
  val rInlineHtmlStart = Pattern.compile("^<(" + blockTags.mkString("|") + ")\\b[^/>]*?>",
    Pattern.MULTILINE | Pattern.CASE_INSENSITIVE)
  // HTML comments
  val rHtmlComment = Pattern.compile("^ {0,3}(<!--.*?-->)\\s*?(?=\\n+|\\Z)",
    Pattern.MULTILINE | Pattern.DOTALL)
  // Link definitions
  val rLinkDefinition = Pattern.compile("^ {0,3}\\[(.+)\\]:" +
      " *\\n? *<?(\\S+)>? *\\n? *" +
      "(?:[\"('](.+?)[\")'])?" +
      "(?=\\n+|\\Z)", Pattern.MULTILINE)
  // Character escaping
  val rEscAmp = Pattern.compile("&(?!#?[xX]?(?:[0-9a-fA-F]+|\\w+);)")
  val rEscLt = Pattern.compile("<(?![a-z/?\\$!])")
  val rInsideTags = Pattern.compile("<(/?" + htmlNameTokenExpr + "(?:\\s+(?:" +
      "(?:" + htmlNameTokenExpr + "\\s*=\\s*\"[^\"]*\")|" +
      "(?:" + htmlNameTokenExpr + "\\s*=\\s*'[^']*')|" +
      "(?:" + htmlNameTokenExpr + "\\s*=\\s*[a-z0-9_:.\\-]+)" +
      ")\\s*)*)>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE)
  // Headers
  val rH1 = Pattern.compile("^ {0,3}(\\S.*?)( *\\{#(.*?)\\})?\\n=+(?=\\n+|\\Z)", Pattern.MULTILINE)
  val rH2 = Pattern.compile("^ {0,3}(\\S.*?)( *\\{#(.*?)\\})?\\n-+(?=\\n+|\\Z)", Pattern.MULTILINE)
  val rHeaders = Pattern.compile("^(#{1,6}) *(\\S.*?)(?: *#*)?( *\\{#(.*?)\\})?$", Pattern.MULTILINE)
  // Horizontal rulers
  val rHr = Pattern.compile("^ {0,3}(?:" +
      "(?:(?:\\* *){3,})|" +
      "(?:(?:- *){3,})|" +
      "(?:(?:_ *){3,})" +
      ") *$", Pattern.MULTILINE)
  val rHtmlHr = Pattern.compile("^ {0,3}(<hr.*?>)\\s*?$",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE)
  // Lists
  val listExpr = "( {0,3}([-+*]|\\d+\\.) +(?s:.+?)" +
      "(?:\\Z|\\n{2,}(?![-+*]|\\s|\\d+\\.)))"
  val rSubList = Pattern.compile("^" + listExpr, Pattern.MULTILINE)
  val rList = Pattern.compile("(?<=\\n\\n|\\A\\n?)" + listExpr, Pattern.MULTILINE)
  val rListItem = Pattern.compile("(\\n)?^( *)(?:[-+*]|\\d+\\.) +" +
      "((?s:.+?)\\n{1,2})(?=\\n*(?:\\Z|\\2(?:[-+*]|\\d+\\.) +))", Pattern.MULTILINE)
  // Code blocks
  val rCodeBlock = Pattern.compile("(?<=\\n\\n|\\A\\n?)" +
      "(^ {4}(?s:.+?))(?=\\Z|\\n+ {0,3}\\S)", Pattern.MULTILINE)
  val rCodeLangId = Pattern.compile("^\\s*lang:(.+?)(?:\\n|\\Z)")
  // Block quotes
  val rBlockQuote = Pattern.compile("((?:^ *>(?:.+(?:\\n|\\Z))+\\n*)+)",
    Pattern.MULTILINE)
  val rBlockQuoteTrims = Pattern.compile("(?:^ *> ?)|(?:^ *$)|(?-m:\\n+$)",
    Pattern.MULTILINE)
  // Paragraphs splitter
  val rParaSplit = Pattern.compile("\\n{2,}")
  // Code spans
  val rCodeSpan = Pattern.compile("(?<!\\\\)(`+)(.+?)(?<!`)\\1(?!`)")
  // Images
  val rImage = Pattern.compile("!\\[(.*?)\\]\\((.*?)( \"(.*?)\")?\\)")
  // Backslash escapes
  val backslashEscapes = ("\\\\\\\\" -> "&#92;") ::
      ("\\\\`" ->  "&#96;") ::
      ("\\\\_" ->  "&#95;") ::
      ("\\\\>" ->  "&gt;") ::
      ("\\\\\\*" ->  "&#42;") ::
      ("\\\\\\{" ->  "&#123;") ::
      ("\\\\\\}" ->  "&#125;") ::
      ("\\\\\\[" ->  "&#91;") ::
      ("\\\\\\]" ->  "&#93;") ::
      ("\\\\\\(" ->  "&#40;") ::
      ("\\\\\\)" ->  "&#41;") ::
      ("\\\\#" ->  "&#35;") ::
      ("\\\\\\+" ->  "&#43;") ::
      ("\\\\-" ->  "&#45;") ::
      ("\\\\\\." ->  "&#46;") ::
      ("\\\\!" ->  "&#33;") :: Nil
  // Reference-style links
  val rRefLinks = Pattern.compile("(\\[(.*?)\\] ?(?:\\n *)?\\[(.*?)\\])")
  // Inline links
  val rInlineLinks = Pattern.compile("\\[(.*?)\\]\\( *<?(.*?)>? *" +
      "((['\"])(.*?)\\4)?\\)", Pattern.DOTALL)
  // Autolinks
  val rAutoLinks = Pattern.compile("<((https?|ftp):[^'\">\\s]+)>")
  // Autoemails
  val rAutoEmail = Pattern.compile("<([-.\\w]+\\@[-a-z0-9]+(\\.[-a-z0-9]+)*\\.[a-z]+)>")
  // Ems and strongs
  val rStrong = Pattern.compile("(\\*\\*|__)(?=\\S)(.+?[*_]*)(?<=\\S)\\1")
  val rEm = Pattern.compile("(\\*|_)(?=\\S)(.+?)(?<=\\S)\\1")
  // Manual linebreaks
  val rBrs = Pattern.compile(" {2,}\n")
  // Ampersand wrapping
  val rAmp = Pattern.compile("&amp;(?!#?[xX]?(?:[0-9a-fA-F]+|\\w+);)")
  // SmartyPants
  val smartyPants = (Pattern.compile("(?<!\\w)(?:\"|&quot;)(?=\\w)") -> leftQuote) ::
      (Pattern.compile("(?<=\\w)(?:\"|&quot;)(?!\\w)") -> rightQuote) ::
      (Pattern.compile("--") -> dash) ::
      (Pattern.compile("\\(r\\)", Pattern.CASE_INSENSITIVE) -> reg) ::
      (Pattern.compile("\\(c\\)", Pattern.CASE_INSENSITIVE) -> copy) ::
      (Pattern.compile("\\(tm\\)", Pattern.CASE_INSENSITIVE) -> trademark) ::
      (Pattern.compile("\\.{3}") -> ellipsis) :: Nil
  // Markdown inside inline HTML
  val rInlineMd = Pattern.compile("<!--#md-->(.*)<!--~+-->", Pattern.DOTALL)

  /**
   * Convert the `source` from Markdown to HTML.
   */
  def apply(source: String): String = new MarkdownText(source).toHtml
}

// # Processing Stuff

/**
 * We collect all processing logic within this class.
 */
class MarkdownText(source: CharSequence) {
  protected var listLevel = 0
  protected var text = new StringEx(source)
  import Markdown._

  /**
   * Link Definitions
   */

  case class LinkDefinition(val url: String, val title: String) {
    override def toString = url + " (" + title + ")"
  }

  protected var links: Map[String, LinkDefinition] = Map()

  // Protector for HTML blocks
  val htmlProtector = new Protector

  // ## Encoding methods

  /**
   * All unsafe chars are encoded to SGML entities.
   */
  protected def encodeUnsafeChars(code: StringEx): StringEx = code
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("*", "&#42;")
      .replaceAll("`", "&#96;")
      .replaceAll("_", "&#95;")
      .replaceAll("\\", "&#92;")

  /**
   * All characters escaped with backslash are encoded to corresponding
   * SGML entities.
   */
  protected def encodeBackslashEscapes(text: StringEx): StringEx =
    backslashEscapes.foldLeft(text)((tx, p) =>
      tx.replaceAll(Pattern.compile(p._1), p._2))

  /**
   * All unsafe chars are encoded to SGML entities inside code blocks.
   */
  protected def encodeCode(code: StringEx): StringEx = code
      .replaceAll(rEscAmp, "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")

  /**
   * Ampersands and less-than signes are encoded to `&amp;` and `&lt;` respectively.
   */
  protected def encodeAmpsAndLts(text: StringEx) = text
      .replaceAll(rEscAmp, "&amp;")
      .replaceAll(rEscLt, "&lt;")

  /**
   * Encodes specially-treated characters inside the HTML tags.
   */
  protected def encodeCharsInsideTags(text: StringEx) =
    text.replaceAll(rInsideTags, m =>
      "<" + encodeUnsafeChars(new StringEx(m.group(1)))
          .replaceAll(rEscAmp, "&amp;")
          .toString + ">")

  // ## Processing methods

  /**
   * Normalization includes following stuff:
   *
   * * replace DOS- and Mac-specific line endings with `\n`;
   * * replace tabs with spaces;
   * * reduce all blank lines (i.e. lines containing only spaces) to empty strings.
   */
  protected def normalize(text: StringEx) = text
      .replaceAll(rLineEnds, "\n")
      .replaceAll(rTabs, "    ")
      .replaceAll(rBlankLines, "")

  /**
   * All inline HTML blocks are hashified, so that no harm is done to their internals.
   */
  protected def hashHtmlBlocks(text: StringEx): StringEx = {
    text.replaceAll(rHtmlHr, m => htmlProtector.addToken(m.group(1)) + "\n")
    val m = text.matcher(rInlineHtmlStart)
    if (m.find) {
      val tagName = m.group(1)
      // This regex will match either opening or closing tag;
      // opening tags will be captured by $1 leaving $2 empty,
      // while closing tags will be captured by $2 leaving $1 empty
      val mTags = text.matcher(Pattern.compile(
        "(<" + tagName + "\\b[^/>]*?>)|(</" + tagName + "\\s*>)",
        Pattern.CASE_INSENSITIVE))
      // Find end index of matching closing tag
      var depth = 1
      var idx = m.end
      while (depth > 0 && idx < text.length && mTags.find(idx)) {
        if (mTags.group(2) == null) depth += 1
        else depth -= 1
        idx = mTags.end
      }
      // Having inline HTML subsequence
      val endIdx = idx
      val startIdx = m.start
      val inlineHtml = new StringEx(text.subSequence(startIdx, endIdx))
      // Process markdown inside
      inlineHtml.replaceAll(rInlineMd, m => new MarkdownText(m.group(1)).toHtml)
      // Hashify block
      val key = htmlProtector.addToken(inlineHtml.toString)
      val sb = new StringBuilder(text.subSequence(0, startIdx))
          .append("\n")
          .append(key)
          .append("\n")
          .append(text.subSequence(endIdx, text.length))
      // Continue recursively until all blocks are processes
      hashHtmlBlocks(new StringEx(sb))
    } else text
  }

  /**
   * All HTML comments are hashified too.
   */
  protected def hashHtmlComments(text: StringEx): StringEx = text.replaceAll(rHtmlComment, m => {
    val comment = m.group(1)
    val hash = htmlProtector.addToken(comment)
    "\n" + hash + "\n"
  })

  /**
   * Standalone link definitions are added to the dictionary and then
   * stripped from the document.
   */
  protected def stripLinkDefinitions(text: StringEx) =
    text.replaceAll(rLinkDefinition, m => {
      val id = m.group(1).toLowerCase
      val url = m.group(2)
      val title = if (m.group(3) == null) "" else m.group(3)
      links += id -> LinkDefinition(url, title.replaceAll("\"", "&quot;"))
      ""
    })

  /**
   * Block elements are processed within specified `text`.
   */
  protected def runBlockGamut(text: StringEx): StringEx = {
    var result = text
    result = doHeaders(result)
    result = doHorizontalRulers(result)
    result = doLists(result)
    result = doCodeBlocks(result)
    result = doBlockQuotes(result)
    result = hashHtmlBlocks(result)    // Again, now hashing our generated markup
    result = formParagraphs(result)
    return result
  }

  /**
   * Process both types of headers.
   */
  protected def doHeaders(text: StringEx): StringEx = text
      .replaceAll(rH1, m => {
    val id = m.group(3)
    val idAttr = if (id == null) "" else " id = \"" + id + "\""
    "<h1" + idAttr + ">" + runSpanGamut(new StringEx(m.group(1))) + "</h1>"
  }).replaceAll(rH2, m => {
    val id = m.group(3)
    val idAttr = if (id == null) "" else " id = \"" + id + "\""
    "<h2" + idAttr + ">" + runSpanGamut(new StringEx(m.group(1))) + "</h2>"
  }).replaceAll(rHeaders, m => {
    val marker = m.group(1)
    val body = runSpanGamut(new StringEx(m.group(2)))
    val id = m.group(4)
    val idAttr = if (id == null) "" else " id = \"" + id + "\""
    "<h" + marker.length + idAttr + ">" + body + "</h" + marker.length + ">"
  })

  /**
   * Process horizontal rulers.
   */
  protected def doHorizontalRulers(text: StringEx): StringEx =
    text.replaceAll(rHr, "\n<hr/>\n")

  /**
   * Process ordered and unordered lists and list items..
   *
   * It is possible to have some nested block elements inside
   * lists, so the contents is passed to `runBlockGamut` after some
   * minor transformations.
   */
  protected def doLists(text: StringEx): StringEx = {
    val pattern = if (listLevel == 0) rList else rSubList
    text.replaceAll(pattern, m => {
      val list = new StringEx(m.group(1))
          .append("\n")
          .replaceAll(rParaSplit, "\n\n\n")
          .replaceAll(rTrailingWS, "\n")
      val listType = m.group(2) match {
        case s if s.matches("[*+-]") => "ul"
        case _ => "ol"
      }
      val result = processListItems(list).replaceAll(rTrailingWS, "")
      "<" + listType + ">\n" + result + "\n</" + listType + ">\n\n"
    })
  }

  protected def processListItems(text: StringEx): StringEx = {
    listLevel += 1
    val sx = text.replaceAll(rListItem, m => {
      val content = m.group(3)
      val leadingLine = m.group(1)
      var item = new StringEx(content).outdent()
      if (leadingLine != null || content.indexOf("\n\n") != -1)
        item = runBlockGamut(item)
      else item = runSpanGamut(doLists(item))
      "<li>" + item.toString.trim + "</li>\n";
    })
    listLevel -= 1
    return sx
  }

  /**
   * Process code blocks.
   */
  protected def doCodeBlocks(text: StringEx): StringEx =
    text.replaceAll(rCodeBlock, m => {
      var langExpr = ""
      val code = encodeCode(new StringEx(m.group(1)))
          .outdent
          .replaceAll(rTrailingWS, "")
          .replaceAll(rCodeLangId, m => {
        langExpr = " class=\"" + m.group(1) + "\""
        ""})
      "<pre" + langExpr + "><code>" + code + "</code></pre>\n\n"
    })

  /**
   * Process blockquotes.
   *
   * It is possible to have some nested block elements inside
   * blockquotes, so the contents is passed to `runBlockGamut` after some
   * minor transformations.
   */
  protected def doBlockQuotes(text: StringEx): StringEx =
    text.replaceAll(rBlockQuote, m => {
      val content = new StringEx(m.group(1))
          .replaceAll(rBlockQuoteTrims, "")
      "<blockquote>\n" + runBlockGamut(content) + "\n</blockquote>\n\n"
    })

  /**
   * At this point all HTML blocks should be hashified, so we treat all lines
   * separated by more than 2 linebreaks as paragraphs.
   */
  protected def formParagraphs(text: StringEx): StringEx = new StringEx(
    rParaSplit.split(text.toString.trim)
        .map(para => htmlProtector.decode(para) match {
      case Some(d) => d
      case _ => "<p>" + runSpanGamut(new StringEx(para)).toString + "</p>"
    }).mkString("\n\n"))

  /**
   * Span elements are processed within specified `text`.
   */
  protected def runSpanGamut(text: StringEx): StringEx = {
    val protector = new Protector
    var result = doCodeSpans(protector, text)
    result = encodeBackslashEscapes(text)
    result = doImages(text)
    result = doRefLinks(text)
    result = doInlineLinks(text)
    result = doAutoLinks(text)
    result = doLineBreaks(text)
    result = protectHtmlTags(protector, text)
    result = doSmartyPants(text)
    result = doAmpSpans(text)
    result = unprotect(protector, text)
    result = doEmphasis(text)
    return result
  }

  protected def protectHtmlTags(protector: Protector, text: StringEx): StringEx =
    text.replaceAll(rInsideTags, m => protector.addToken(m.group(0)))

  protected def unprotect(protector: Protector, text: StringEx): StringEx =
    protector.keys.foldLeft(text)((t, k) => t.replaceAll(k, protector.decode(k).getOrElse("")))

  /**
   * Process code spans.
   */
  protected def doCodeSpans(protector: Protector, text: StringEx): StringEx = text.replaceAll(rCodeSpan, m =>
    protector.addToken("<code>" + encodeCode(new StringEx(m.group(2).trim)) + "</code>"))

  /**
   * Process images.
   */
  protected def doImages(text: StringEx): StringEx = text.replaceAll(rImage, m => {
    val alt = m.group(1)
    val src = m.group(2)
    val title = m.group(4)
    var result = "<img src=\"" + src + "\" alt=\"" + alt + "\""
    if (title != null && title != "")
      result += " title=\"" + title + "\""
    result + "/>"
  })

  /**
   * Process reference-style links.
   */
  protected def doRefLinks(text: StringEx): StringEx = text.replaceAll(rRefLinks, m => {
    val wholeMatch = m.group(1)
    val linkText = m.group(2)
    var id = m.group(3).trim.toLowerCase
    if (id == null || id == "") id = linkText.trim.toLowerCase
    links.get(id) match {
      case Some(ld) =>
        val title = ld.title
            .replaceAll("\\*", "&#42;")
            .replaceAll("_", "&#95;")
            .trim
        val url = ld.url
            .replaceAll("\\*", "&#42;")
            .replaceAll("_", "&#95;")
        val titleAttr = if (title != "") " title=\"" + title + "\"" else ""
        "<a href=\"" + url + "\"" + titleAttr + ">" + linkText + "</a>"
      case _ => wholeMatch
    }
  })

  /**
   * Process inline links.
   */
  protected def doInlineLinks(text: StringEx): StringEx =
    text.replaceAll(rInlineLinks, m => {
      val linkText = m.group(1)
      val url = m.group(2)
          .replaceAll("\\*", "&#42;")
          .replaceAll("_", "&#95;")
      var titleAttr = ""
      var title = m.group(5)
      if (title != null) titleAttr = " title=\"" + title
          .replaceAll("\\*", "&#42;")
          .replaceAll("_", "&#95;")
          .replaceAll("\"", "&quot;")
          .trim + "\""
      "<a href=\"" + url + "\"" + titleAttr + ">" + linkText + "</a>"
    })

  /**
   * Process autolinks.
   */
  protected def doAutoLinks(text: StringEx): StringEx = text
      .replaceAll(rAutoLinks, m => "<a href=\"" + m.group(1) + "\">" + m.group(1) + "</a>")
      .replaceAll(rAutoEmail, m => {
    val address = m.group(1)
    val url = "mailto:" + address
    "<a href=\"" + encodeEmail(url) + "\">" + encodeEmail(address) + "</a>"
  })

  /**
   * Process autoemails in anti-bot manner.
   */
  protected def encodeEmail(s: String) = s.toList.map(c => {
    val r = rnd.nextDouble
    if (r < 0.45) "&#" + c.toInt + ";"
    else if (r < 0.9) "&#x" + Integer.toHexString(c.toInt) + ";"
    else c
  }).mkString

  /**
   * Process EMs and STRONGs.
   */
  protected def doEmphasis(text: StringEx): StringEx = text
      .replaceAll(rStrong, m => "<strong>" + m.group(2) + "</strong>")
      .replaceAll(rEm, m => "<em>" + m.group(2) + "</em>")

  /**
   * Process manual linebreaks.
   */
  protected def doLineBreaks(text: StringEx): StringEx = text
      .replaceAll(rBrs, " <br/>\n")

  /**
   * Process SmartyPants stuff.
   */
  protected def doSmartyPants(text: StringEx): StringEx =
    smartyPants.foldLeft(text)((t,p) => t.replaceAll(p._1, p._2))

  /**
   * Wrap ampersands with `<span class="amp">`.
   */
  protected def doAmpSpans(text: StringEx): StringEx =
    text.replaceAll(rAmp, "<span class=\"amp\">&amp;</span>")

  /**
   * Transform the Markdown source into HTML.
   */
  def toHtml(): String = {
    var result = text
    result = normalize(result)
    result = encodeCharsInsideTags(result)
    result = hashHtmlBlocks(result)
    result = hashHtmlComments(result)
    result = encodeAmpsAndLts(result)
    result = stripLinkDefinitions(result)
    result = runBlockGamut(result)
    return result.toString
  }

}
