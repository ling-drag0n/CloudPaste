// 导入docx库组件用于创建Word文档
import {
  Document,
  Packer,
  Paragraph,
  TextRun,
  HeadingLevel,
  AlignmentType,
  ImageRun,
  Table,
  TableRow,
  TableCell,
  WidthType,
  BorderStyle,
  Tab,
  VerticalAlign,
  ExternalHyperlink,
  UnderlineType,
  ShadingType,
  Header,
  Footer,
  Math as MathElement,
  MathRun,
} from "docx";

/**
 * 将Markdown转换为Word文档并返回Blob对象
 * @param {string} markdownContent - Markdown格式的内容
 * @param {Object} options - 转换选项
 * @returns {Promise<Blob>} - 返回包含Word文档的Blob对象
 */
export async function markdownToWord(markdownContent, options = {}) {
  // 默认文档标题
  const title = options.title || "导出文档";

  // 解析Markdown内容，生成文档组件
  const docElements = await parseMarkdownToDocElements(markdownContent);

  // 创建Word文档
  const doc = new Document({
    title: title,
    description: "由Markdown导出的文档",
    sections: [
      {
        properties: {},
        children: docElements,
      },
    ],
  });

  // 将文档转换为Blob
  return await Packer.toBlob(doc);
}

/**
 * 解析Markdown内容，生成Word文档元素数组
 * @param {string} markdown - Markdown内容
 * @returns {Promise<Array>} - 返回Word文档元素数组
 */
async function parseMarkdownToDocElements(markdown) {
  // 将Markdown按行分割
  const lines = markdown.split("\n");
  const elements = [];

  // 状态变量
  let inCodeBlock = false;
  let codeBlockContent = [];
  let codeBlockLanguage = "";
  let inBlockQuote = false;
  let blockQuoteLines = [];
  let inList = false;
  let listItems = [];
  let listType = ""; // 'ul' 或 'ol'
  let listLevel = 0;
  let inTable = false;
  let tableRows = [];
  let headerSeparatorLine = "";
  let inMathBlock = false; // 是否在数学公式块中
  let mathBlockContent = []; // 数学公式块内容

  // 逐行处理Markdown内容
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const nextLine = i < lines.length - 1 ? lines[i + 1] : "";

    // 处理代码块
    if (line.trim().startsWith("```")) {
      if (!inCodeBlock) {
        // 代码块开始
        inCodeBlock = true;
        codeBlockContent = [];
        // 提取语言（去除前面可能的空格）
        codeBlockLanguage = line.trim().slice(3).trim();
      } else {
        // 代码块结束
        inCodeBlock = false;
        // 检查是否有内容，然后创建代码块
        if (codeBlockContent.length > 0) {
          elements.push(...createCodeBlockAlternative(codeBlockContent.join("\n"), codeBlockLanguage));
        } else {
          // 空代码块，添加一个占位符
          elements.push(...createCodeBlockAlternative(" ", codeBlockLanguage));
        }
        codeBlockContent = [];
        codeBlockLanguage = "";
      }
      continue;
    }

    if (inCodeBlock) {
      // 收集代码块内容 - 保持原始格式不变
      codeBlockContent.push(line);
      continue;
    }

    // 处理表格
    // 识别表格行（以|开头或结尾的行，并且不是分隔行）
    const isTableRow = (line.trim().startsWith("|") || line.trim().endsWith("|")) && line.trim().length > 1;
    // 识别表格分隔行（由|-和:组成）
    const isTableSeparator =
      isTableRow &&
      line
        .replace(/\|/g, "")
        .trim()
        .split("")
        .every((char) => char === "-" || char === ":" || char === " ");

    if (isTableRow) {
      if (!inTable) {
        // 表格开始
        inTable = true;
        tableRows = [];
      }

      // 如果是表格分隔行，保存用于确定对齐方式
      if (isTableSeparator) {
        headerSeparatorLine = line;
      } else {
        // 否则，添加为常规的表格行
        tableRows.push(line);
      }

      // 如果没有下一行，或下一行不是表格行，则结束表格处理
      if ((!nextLine.trim().startsWith("|") && !nextLine.trim().endsWith("|")) || i === lines.length - 1) {
        inTable = false;

        // 如果有有效的表格行和分隔符，创建表格
        if (tableRows.length > 0 && headerSeparatorLine) {
          elements.push(createTable(tableRows, headerSeparatorLine));
          tableRows = [];
          headerSeparatorLine = "";
        } else {
          // 如果缺少分隔符，按普通段落处理
          for (const row of tableRows) {
            elements.push(createParagraph(row));
          }
          tableRows = [];
          headerSeparatorLine = "";
        }
      }
      continue;
    }

    // 处理引用块
    if (line.startsWith(">")) {
      if (!inBlockQuote) {
        inBlockQuote = true;
        blockQuoteLines = [];
      }

      // 计算引用的嵌套级别
      let quoteLevel = 0;
      let trimmedLine = line;

      // 计算嵌套级别，每个 > 符号增加一级
      while (trimmedLine.startsWith(">")) {
        quoteLevel++;
        trimmedLine = trimmedLine.slice(1).trim();
      }

      // 添加内容，保留嵌套信息
      blockQuoteLines.push({
        content: trimmedLine,
        level: quoteLevel,
      });

      // 如果下一行不是引用，或者已经是最后一行
      if (!nextLine.startsWith(">") || i === lines.length - 1) {
        inBlockQuote = false;
        elements.push(createBlockQuote(blockQuoteLines));
        blockQuoteLines = [];
      }
      continue;
    }

    // 处理列表
    if (line.match(/^(\s*)([-*+]|\d+\.)\s/)) {
      if (!inList) {
        inList = true;
        listItems = [];
        listType = line.match(/^\s*[-*+]\s/) ? "ul" : "ol";
      }

      // 识别普通列表项
      let isTask = false;
      let checked = false;

      // 检查是否是任务列表项 - 匹配 [x], [X], [ ] 格式
      const taskMatch = line.match(/^(\s*)([-*+])\s+\[([ xX])\]\s(.*)/);

      if (taskMatch) {
        // 这是一个任务列表项
        const indent = taskMatch[1].length;
        const marker = taskMatch[3].toLowerCase(); // 'x' 或 ' '
        const content = taskMatch[4];
        const level = Math.floor(indent / 2); // 每两个空格增加一个缩进级别

        checked = marker === "x";
        isTask = true;

        listItems.push({ content, level, isTask, checked });
      } else {
        // 普通列表项处理
        const match = line.match(/^(\s*)([-*+]|\d+\.)\s(.*)/);
        if (match) {
          const indent = match[1].length;
          const content = match[3];
          const level = Math.floor(indent / 2); // 每两个空格增加一个缩进级别

          listItems.push({ content, level, isTask, checked });
        }
      }

      // 如果下一行不是列表，或者已经是最后一行
      if (nextLine.trim() === "" || !nextLine.match(/^(\s*)([-*+]|\d+\.)\s/) || i === lines.length - 1) {
        inList = false;
        elements.push(...createListItems(listItems, listType));
        listItems = [];
      }
      continue;
    }

    // 处理标题
    if (line.startsWith("#")) {
      const match = line.match(/^(#{1,6})\s+(.+)$/);
      if (match) {
        const level = match[1].length;
        const text = match[2].trim();
        elements.push(createHeading(text, level));
      }
      continue;
    }

    // 处理水平线
    if (line.match(/^(\*{3,}|-{3,}|_{3,})$/)) {
      elements.push(createHorizontalRule());
      continue;
    }

    // 处理空行
    if (line.trim() === "") {
      // 如果不是连续空行，添加一个空段落
      if (elements.length > 0 && !(elements[elements.length - 1] instanceof Paragraph && elements[elements.length - 1].text === "")) {
        elements.push(new Paragraph({}));
      }
      continue;
    }

    // 处理数学公式块 (独立的$$行)
    if (line.trim() === "$$") {
      if (!inMathBlock) {
        // 数学公式块开始
        inMathBlock = true;
        mathBlockContent = [];
      } else {
        // 数学公式块结束
        inMathBlock = false;
        // 合并公式内容并创建公式块
        if (mathBlockContent.length > 0) {
          const formula = mathBlockContent.join("\n").trim();
          const mathParagraph = new Paragraph({
            children: [createMathEquation(formula, true)],
            alignment: AlignmentType.CENTER,
            spacing: {
              before: 240,
              after: 240,
            },
          });
          elements.push(mathParagraph);
        } else {
          // 空公式块，添加一个占位符
          elements.push(new Paragraph({}));
        }
        mathBlockContent = [];
      }
      continue;
    }

    if (inMathBlock) {
      // 收集数学公式块内容
      mathBlockContent.push(line);
      continue;
    }

    // 处理单行块级数学公式 ($$...$$ 格式)
    if (line.trim().startsWith("$$") && line.trim().endsWith("$$") && line.trim() !== "$$") {
      // 提取公式内容，去除首尾的$$
      const formula = line.trim().slice(2, -2).trim();
      if (formula) {
        // 创建数学公式段落
        const mathParagraph = new Paragraph({
          children: [createMathEquation(formula, true)],
          alignment: AlignmentType.CENTER,
          spacing: {
            before: 240,
            after: 240,
          },
        });
        elements.push(mathParagraph);
      } else {
        // 空公式，添加一个占位符
        elements.push(new Paragraph({}));
      }
      continue;
    }

    // 处理普通段落
    elements.push(createParagraph(line));
  }

  return elements;
}

/**
 * 创建标题
 * @param {string} text - 标题文本
 * @param {number} level - 标题级别 (1-6)
 * @returns {Paragraph} - 标题段落
 */
function createHeading(text, level) {
  let headingLevel;

  switch (level) {
    case 1:
      headingLevel = HeadingLevel.HEADING_1;
      break;
    case 2:
      headingLevel = HeadingLevel.HEADING_2;
      break;
    case 3:
      headingLevel = HeadingLevel.HEADING_3;
      break;
    case 4:
      headingLevel = HeadingLevel.HEADING_4;
      break;
    case 5:
      headingLevel = HeadingLevel.HEADING_5;
      break;
    case 6:
      headingLevel = HeadingLevel.HEADING_6;
      break;
    default:
      headingLevel = HeadingLevel.HEADING_1;
  }

  return new Paragraph({
    text: text,
    heading: headingLevel,
  });
}

/**
 * 创建段落，处理内联样式如粗体、斜体、链接
 * @param {string} text - 段落文本
 * @returns {Paragraph} - 格式化的段落
 */
function createParagraph(text) {
  const children = [];

  // 处理文本中的内联样式
  let remainingText = text;
  let position = 0;

  // 查找Markdown内联样式
  const patterns = [
    { regex: /\*\*(.+?)\*\*/g, type: "bold" },
    { regex: /\*(.+?)\*/g, type: "italic" },
    { regex: /_(.+?)_/g, type: "italic" },
    { regex: /~~(.+?)~~/g, type: "strikethrough" },
    { regex: /`(.+?)`/g, type: "code" },
    { regex: /\[(.+?)\]\((.+?)\)/g, type: "link" },
    { regex: /!\[(.+?)\]\((.+?)\)/g, type: "image" },
    { regex: /\$(.+?)\$/g, type: "math" },
  ];

  // 用于临时存储所有匹配项
  const matches = [];

  // 找出所有匹配项
  patterns.forEach((pattern) => {
    let match;
    while ((match = pattern.regex.exec(text)) !== null) {
      matches.push({
        start: match.index,
        end: match.index + match[0].length,
        content: match[0],
        innerContent: match[1],
        type: pattern.type,
        url: pattern.type === "link" || pattern.type === "image" ? match[2] : null,
      });
    }
  });

  // 按开始位置排序匹配项
  matches.sort((a, b) => a.start - b.start);

  // 检查重叠并拆分匹配项
  const cleanMatches = [];
  for (let i = 0; i < matches.length; i++) {
    let current = matches[i];
    let isOverlapping = false;

    for (let j = 0; j < cleanMatches.length; j++) {
      const existing = cleanMatches[j];

      // 检查是否重叠
      if (!(current.end <= existing.start || current.start >= existing.end)) {
        isOverlapping = true;
        break;
      }
    }

    if (!isOverlapping) {
      cleanMatches.push(current);
    }
  }

  // 再次按开始位置排序
  cleanMatches.sort((a, b) => a.start - b.start);

  // 处理普通文本和匹配项
  let lastEnd = 0;

  for (const match of cleanMatches) {
    // 添加匹配前的普通文本
    if (match.start > lastEnd) {
      children.push(
        new TextRun({
          text: text.substring(lastEnd, match.start),
        })
      );
    }

    // 添加特殊格式文本
    switch (match.type) {
      case "bold":
        children.push(
          new TextRun({
            text: match.innerContent,
            bold: true,
          })
        );
        break;
      case "italic":
        children.push(
          new TextRun({
            text: match.innerContent,
            italics: true,
          })
        );
        break;
      case "strikethrough":
        children.push(
          new TextRun({
            text: match.innerContent,
            strike: true,
          })
        );
        break;
      case "code":
        children.push(
          new TextRun({
            text: match.innerContent,
            font: "Consolas, 'Courier New', monospace",
            size: 20, // 10pt
            color: "7A3E9D", // 紫色，常用于代码字符
            shading: {
              type: ShadingType.SOLID,
              color: "F5F5F5", // 浅灰色背景
            },
          })
        );
        break;
      case "math":
        // 处理行内数学公式
        children.push(createMathEquation(match.innerContent, false));
        break;
      case "link":
        children.push(
          new ExternalHyperlink({
            children: [
              new TextRun({
                text: match.innerContent,
                style: "Hyperlink",
                color: "0000FF",
                underline: {
                  type: UnderlineType.SINGLE,
                },
              }),
            ],
            link: match.url,
          })
        );
        break;
      case "image":
        // 图片需要特殊处理，因为无法直接在TextRun中使用
        // 这里只添加一个占位符，实际图片处理需要在上层进行
        children.push(
          new TextRun({
            text: `[图片: ${match.innerContent}]`,
            italics: true,
          })
        );
        break;
    }

    lastEnd = match.end;
  }

  // 添加剩余的普通文本
  if (lastEnd < text.length) {
    children.push(
      new TextRun({
        text: text.substring(lastEnd),
      })
    );
  }

  // 如果没有特殊格式，直接返回普通文本段落
  if (children.length === 0) {
    return new Paragraph({
      children: [
        new TextRun({
          text: text,
        }),
      ],
    });
  }

  return new Paragraph({
    children: children,
  });
}

/**
 * 创建代码块的备选实现 - 使用多行段落表示代码块
 * 此函数解决了原始createCodeBlock函数中代码行被挤在一起的问题。
 * 通过为每行代码创建独立的段落，确保在Word文档中正确保留格式和换行。
 *
 * 实现原理：
 * 1. 不同于原始实现在单个段落中使用TextRun的break属性
 * 2. 此实现为每行代码创建单独的段落，可以更精确地控制每行的格式
 * 3. 通过精细控制段落间距和边框，使多个段落在视觉上形成一个整体代码块
 * 4. 对前导空格使用非断行空格，确保代码缩进正确显示
 *
 * @param {string} code - 代码内容
 * @param {string} language - 编程语言
 * @returns {Array<Paragraph>} - 格式化的代码段落数组
 */
function createCodeBlockAlternative(code, language) {
  // 标准化语言名称
  const normalizedLanguage = language ? language.toLowerCase() : "";

  // 将代码分割为行
  const codeLines = code.split("\n");

  // 确定背景颜色 - 根据不同语言可以使用不同颜色
  let backgroundColor = "F5F5F5"; // 默认浅灰色背景
  if (["javascript", "js", "typescript", "ts"].includes(normalizedLanguage)) {
    backgroundColor = "FFFDF0";
  } else if (["html", "xml", "svg"].includes(normalizedLanguage)) {
    backgroundColor = "FFF0F0";
  } else if (["css", "scss", "sass", "less"].includes(normalizedLanguage)) {
    backgroundColor = "F0FFFF";
  } else if (["python", "py"].includes(normalizedLanguage)) {
    backgroundColor = "F0F8FF";
  }

  // 准备代码样式
  const codeStyle = {
    font: "Consolas, 'Courier New', monospace",
    size: 20,
    color: "333333",
  };

  // 准备要返回的段落数组
  const paragraphs = [];

  // 创建通用段落属性（所有代码行共享的属性）
  const paragraphProperties = {
    shading: {
      type: ShadingType.SOLID,
      color: backgroundColor,
    },
    spacing: {
      before: 0,
      after: 0,
      line: 240, // 标准行距
      lineRule: "exact",
    },
    border: {
      left: { style: BorderStyle.SINGLE, size: 3, color: "A0A0A0" },
    },
    indent: {
      left: 360,
    },
  };

  // 添加语言标题段落（如果有）
  if (language) {
    const displayLanguage = language.toUpperCase();
    paragraphs.push(
      new Paragraph({
        ...paragraphProperties,
        border: {
          ...paragraphProperties.border,
          top: { style: BorderStyle.SINGLE, size: 1, color: "E0E0E0" },
        },
        spacing: {
          ...paragraphProperties.spacing,
          before: 240, // 第一行前额外空间
        },
        children: [
          new TextRun({
            text: displayLanguage,
            bold: true,
            color: "606060",
            font: codeStyle.font,
            size: codeStyle.size,
          }),
        ],
      })
    );
  }

  // 处理每一行代码
  codeLines.forEach((line, index) => {
    // 处理前导空格
    let formattedLine = "";
    let nonSpaceFound = false;

    // 逐字符处理，确保前导空格被替换为非断行空格
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (!nonSpaceFound && char === " ") {
        formattedLine += "\u00A0"; // 非断行空格
      } else {
        nonSpaceFound = true;
        formattedLine += char;
      }
    }

    // 如果是空行，确保至少有一个非断行空格保持行高
    if (formattedLine === "") {
      formattedLine = "\u00A0";
    }

    // 创建当前行的段落
    const isLastLine = index === codeLines.length - 1;
    const isFirstCode = index === 0;

    paragraphs.push(
      new Paragraph({
        ...paragraphProperties,
        // 边框处理 - 为首尾行添加特殊边框，所有行都有左右边框
        border: {
          ...paragraphProperties.border,
          // 顶部边框只在没有语言标题或第一行时显示
          ...(isFirstCode && !language ? { top: { style: BorderStyle.SINGLE, size: 1, color: "E0E0E0" } } : {}),
          // 底部边框只在最后一行显示
          ...(isLastLine ? { bottom: { style: BorderStyle.SINGLE, size: 1, color: "E0E0E0" } } : {}),
          // 所有行都有右边框
          right: { style: BorderStyle.SINGLE, size: 1, color: "E0E0E0" },
        },
        // 只在首尾行添加额外间距
        spacing: {
          ...paragraphProperties.spacing,
          before: isFirstCode && !language ? 240 : 0, // 首行额外上间距（如果没有语言标题）
          after: isLastLine ? 240 : 0, // 尾行额外下间距
        },
        children: [
          new TextRun({
            text: formattedLine,
            font: codeStyle.font,
            size: codeStyle.size,
            color: codeStyle.color,
            noProof: true,
          }),
        ],
      })
    );
  });

  return paragraphs;
}

/**
 * 创建块引用
 * @param {Array} quoteLines - 引用内容数组，每项包含内容和嵌套级别
 * @returns {Paragraph} - 格式化的引用段落
 */
function createBlockQuote(quoteLines) {
  // 处理嵌套引用
  let processedText = "";
  let currentLevel = 1;

  // 处理每一行，根据嵌套级别添加缩进和引用符号
  quoteLines.forEach((line, index) => {
    // 根据嵌套级别添加额外缩进和引用符号
    const indentPrefix = "    ".repeat(line.level - 1); // 每增加一级缩进4个空格
    const quotePrefix = line.level > 1 ? "» " : ""; // 嵌套引用使用特殊符号

    processedText += indentPrefix + quotePrefix + line.content;

    // 如果不是最后一行，添加换行符
    if (index < quoteLines.length - 1) {
      processedText += "\n";
    }
  });

  // 找到最大嵌套级别，用于确定左边框颜色
  const maxLevel = Math.max(...quoteLines.map((line) => line.level));

  // 为嵌套级别选择不同的边框颜色
  let borderColor = "CCCCCC"; // 默认颜色
  if (maxLevel > 1) {
    // 根据嵌套深度，使边框颜色更深
    const colorDepth = Math.min(maxLevel - 1, 3); // 最多支持4级嵌套颜色变化
    const colors = ["CCCCCC", "AAAAAA", "888888", "666666"];
    borderColor = colors[colorDepth];
  }

  return new Paragraph({
    indent: {
      left: 720, // 基础左侧缩进
    },
    border: {
      left: { style: BorderStyle.SINGLE, size: 3, color: borderColor },
    },
    children: [
      new TextRun({
        text: processedText,
        italics: true,
        color: "666666",
      }),
    ],
  });
}

/**
 * 创建列表项
 * @param {Array} items - 列表项数组，每项包含内容和缩进级别，以及任务相关属性
 * @param {string} type - 列表类型 ('ul'或'ol')
 * @returns {Array<Paragraph>} - 格式化的列表段落数组
 */
function createListItems(items, type) {
  return items.map((item, index) => {
    const bulletChar = type === "ul" ? "•" : `${index + 1}.`;

    // 定义段落子元素
    const children = [];

    // 如果是任务列表项，添加任务复选框
    if (item.isTask) {
      const checkboxChar = item.checked ? "☑" : "☐";

      // 添加复选框
      children.push(
        new TextRun({
          text: checkboxChar + " ",
          bold: true,
        })
      );

      // 如果已完成，添加删除线效果
      if (item.checked) {
        children.push(
          new TextRun({
            text: item.content,
            strike: true,
            color: "909090", // 灰色文本表示已完成
          })
        );
      } else {
        children.push(
          new TextRun({
            text: item.content,
          })
        );
      }
    } else {
      // 普通列表项
      children.push(
        new TextRun({
          text: item.content,
        })
      );
    }

    return new Paragraph({
      indent: {
        left: item.level * 720 + 720, // 根据级别增加缩进
      },
      bullet: {
        level: item.level,
      },
      children: children,
    });
  });
}

/**
 * 创建水平分隔线
 * @returns {Paragraph} - 格式化的水平线段落
 */
function createHorizontalRule() {
  return new Paragraph({
    thematicBreak: true,
    spacing: {
      before: 240,
      after: 240,
    },
  });
}

/**
 * 处理图片转换
 * 注意：此功能受限于docx库和浏览器API的限制
 * @param {string} imageUrl - 图片URL
 * @param {string} altText - 替代文本
 * @returns {Promise<ImageRun>} - 图片对象
 */
async function processImage(imageUrl, altText) {
  try {
    // 获取图片数据
    const response = await fetch(imageUrl);
    const blob = await response.blob();

    // 将Blob转换为ArrayBuffer
    const arrayBuffer = await blob.arrayBuffer();

    // 创建图片对象
    return new ImageRun({
      data: arrayBuffer,
      transformation: {
        width: 400, // 设置默认宽度
        height: 300, // 设置默认高度
      },
      altText: altText,
    });
  } catch (error) {
    console.error("处理图片时出错:", error);
    // 返回一个文本作为替代
    return new TextRun({
      text: `[无法加载图片: ${altText}]`,
      italics: true,
    });
  }
}

/**
 * 创建表格
 * @param {Array<string>} rows - 表格行数组
 * @param {string} separatorLine - 表格分隔行，用于确定对齐方式
 * @returns {Table} - Word表格对象
 */
function createTable(rows, separatorLine) {
  // 解析表格行
  const tableRows = parseTableRows(rows);
  // 获取列对齐信息
  const alignments = getColumnAlignments(separatorLine);

  // 创建表格
  const table = new Table({
    width: {
      size: 100,
      type: WidthType.PERCENTAGE,
    },
    borders: {
      top: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
      bottom: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
      left: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
      right: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
      insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
      insideVertical: { style: BorderStyle.SINGLE, size: 1, color: "auto" },
    },
    rows: tableRows.map((row, rowIndex) => {
      // 创建表格行
      return new TableRow({
        tableHeader: rowIndex === 0, // 第一行是表头
        children: row.map((cell, colIndex) => {
          // 获取单元格对齐方式
          const alignment = colIndex < alignments.length ? alignments[colIndex] : AlignmentType.LEFT;

          // 创建表格单元格
          return new TableCell({
            children: [
              new Paragraph({
                children: [
                  new TextRun({
                    text: cell,
                    bold: rowIndex === 0, // 表头加粗
                  }),
                ],
                alignment: alignment, // 应用对齐方式
              }),
            ],
            shading:
              rowIndex === 0
                ? {
                    fill: "F2F2F2", // 表头背景色
                    val: "clear",
                    color: "auto",
                  }
                : undefined,
            margins: {
              top: 100,
              bottom: 100,
              left: 150,
              right: 150,
            },
            verticalAlign: VerticalAlign.CENTER,
          });
        }),
      });
    }),
  });

  return table;
}

/**
 * 解析表格行
 * @param {Array<string>} rows - 表格行数组
 * @returns {Array<Array<string>>} - 二维数组，包含解析后的单元格内容
 */
function parseTableRows(rows) {
  // 处理每一行
  const parsedRows = rows.map((row) => {
    // 移除行首尾的|，然后按|分割
    let cells = row
      .trim()
      .replace(/^\||\|$/g, "")
      .split("|");

    // 处理每个单元格
    return cells.map((cell) => cell.trim());
  });

  // 确保所有行的列数相同（使用最大列数）
  const maxColumns = Math.max(...parsedRows.map((row) => row.length));

  // 填充缺少的单元格
  return parsedRows.map((row) => {
    // 如果当前行的列数小于最大列数，补充空单元格
    if (row.length < maxColumns) {
      const paddedRow = [...row];
      while (paddedRow.length < maxColumns) {
        paddedRow.push("");
      }
      return paddedRow;
    }
    return row;
  });
}

/**
 * 获取列对齐信息
 * @param {string} separatorLine - 表格分隔行
 * @returns {Array<string>} - 每列的对齐方式
 */
function getColumnAlignments(separatorLine) {
  // 移除行首尾的|，然后按|分割
  const separators = separatorLine
    .trim()
    .replace(/^\||\|$/g, "")
    .split("|");

  return separators.map((separator) => {
    separator = separator.trim();

    // 检查对齐标记
    if (separator.startsWith(":") && separator.endsWith(":")) {
      return AlignmentType.CENTER;
    } else if (separator.endsWith(":")) {
      return AlignmentType.RIGHT;
    } else {
      return AlignmentType.LEFT;
    }
  });
}

/**
 * 创建数学公式
 * @param {string} formula - LaTeX格式的数学公式
 * @param {boolean} isBlock - 是否为块级公式
 * @returns {MathElement} - Word数学公式对象
 */
function createMathEquation(formula, isBlock = false) {
  return new MathElement({
    children: [new MathRun(formula)],
    // 块级公式居中显示
    alignment: isBlock ? AlignmentType.CENTER : AlignmentType.LEFT,
  });
}

export default markdownToWord;
