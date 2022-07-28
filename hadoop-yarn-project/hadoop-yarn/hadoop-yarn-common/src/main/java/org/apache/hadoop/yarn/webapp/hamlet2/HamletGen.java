/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.webapp.hamlet2;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.WebAppException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a specific hamlet implementation class from a spec class
 * using a generic hamlet implementation class.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class HamletGen {
  static final Logger LOG = LoggerFactory.getLogger(HamletGen.class);
  static final Options opts = new Options();
  static {
    opts.addOption("h", "help", false, "Print this help message").
         addOption("s", "spec-class", true,
                   "The class that holds the spec interfaces. e.g. HamletSpec").
         addOption("i", "impl-class", true,
                   "An implementation class. e.g. HamletImpl").
         addOption("o", "output-class", true, "Output class name").
         addOption("p", "output-package", true, "Output package name");
  };

  static final Pattern elementRegex = Pattern.compile("^[A-Z][A-Z0-9]*$");

  int bytes = 0;
  PrintWriter out;
  final Set<String> endTagOptional = Sets.newHashSet();
  final Set<String> inlineElements = Sets.newHashSet();
  Class<?> top; // html top-level interface
  String hamlet; // output class simple name;
  boolean topMode;

  /**
   * Generate a specific Hamlet implementation from a spec.
   * @param specClass holds hamlet interfaces. e.g. {@link HamletSpec}
   * @param implClass a generic hamlet implementation. e.g. {@link HamletImpl}
   * @param outputName name of the output class. e.g. {@link Hamlet}
   * @param outputPkg package name of the output class.
   * @throws IOException
   */
  public void generate(Class<?> specClass, Class<?> implClass,
                       String outputName, String outputPkg) throws IOException {
    LOG.info("Generating {} using {} and {}", new Object[]{outputName,
             specClass, implClass});
    out = new PrintWriter(outputName +".java", "UTF-8");
    hamlet = basename(outputName);
    String pkg = pkgName(outputPkg, implClass.getPackage().getName());
    puts(0, "// Generated by HamletGen. Do NOT edit!\n",
         "package ", pkg, ";\n",
         "import java.io.PrintWriter;\n",
         "import java.util.EnumSet;\n",
         "import static java.util.EnumSet.*;\n",
         "import static ", implClass.getName(), ".EOpt.*;\n",
         "import org.apache.hadoop.yarn.webapp.SubView;");
    String implClassName = implClass.getSimpleName();
    if (!implClass.getPackage().getName().equals(pkg)) {
      puts(0, "import ", implClass.getName(), ';');
    }
    puts(0, "\n",
         "public class ", hamlet, " extends ", implClassName,
         " implements ", specClass.getSimpleName(), "._Html {\n",
         "  public ", hamlet, "(PrintWriter out, int nestLevel,",
         " boolean wasInline) {\n",
         "    super(out, nestLevel, wasInline);\n",
         "  }\n\n", // inline is context sensitive
         "  static EnumSet<EOpt> opt(boolean endTag, boolean inline, ",
         "boolean pre) {\n",
         "    EnumSet<EOpt> opts = of(ENDTAG);\n",
         "    if (!endTag) opts.remove(ENDTAG);\n",
         "    if (inline) opts.add(INLINE);\n",
         "    if (pre) opts.add(PRE);\n",
         "    return opts;\n",
         "  }");
    initLut(specClass);
    genImpl(specClass, implClassName, 1);
    LOG.error("Temp", new RuntimeException());
    genMethods(hamlet, top, 1);
    puts(0, "}");
    out.close();
    LOG.error("Temp", new RuntimeException());
  }

  String basename(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  String pkgName(String pkg, String defaultPkg) {
    if (pkg == null || pkg.isEmpty()) return defaultPkg;
    return pkg;
  }

  void initLut(Class<?> spec) {
    endTagOptional.clear();
    inlineElements.clear();
    for (Class<?> cls : spec.getClasses()) {
      Annotation a = cls.getAnnotation(HamletSpec.Element.class);
      if (a != null && !((HamletSpec.Element) a).endTag()) {
        endTagOptional.add(cls.getSimpleName());
      }
      if (cls.getSimpleName().equals("Inline")) {
        for (Method method : cls.getMethods()) {
          String retName = method.getReturnType().getSimpleName();
          if (isElement(retName)) {
            inlineElements.add(retName);
          }
        }
      }
    }
  }

  void genImpl(Class<?> spec, String implClassName, int indent) {
    String specName = spec.getSimpleName();
    for (Class<?> cls : spec.getClasses()) {
      String className = cls.getSimpleName();
      if (cls.isInterface()) {
        genFactoryMethods(cls, indent);
      }
      if (isElement(className)) {
        LOG.error("Temp", new RuntimeException());
        puts(indent, "\n",
             "public class ", className, "<T extends __>",
             " extends EImp<T> implements ", specName, ".", className, " {\n",
             "  public ", className, "(String name, T parent,",
             " EnumSet<EOpt> opts) {\n",
             "    super(name, parent, opts);\n",
             "  }");
        genMethods(className, cls, indent + 1);
        puts(indent, "}");
      } else if (className.equals("_Html")) {
        top = cls;
      }
    }
  }

  void genFactoryMethods(Class<?> cls, int indent) {
    for (Method method : cls.getDeclaredMethods()) {
      String retName = method.getReturnType().getSimpleName();
      String methodName = method.getName();
      if (methodName.charAt(0) == '$') continue;
      if (isElement(retName) && method.getParameterTypes().length == 0) {
        genFactoryMethod(retName, methodName, indent);
      }
    }
  }

  void genMethods(String className, Class<?> cls, int indent) {
    topMode = (top != null && cls.equals(top));
    for (Method method : cls.getMethods()) {
      String retName = method.getReturnType().getSimpleName();
      if (method.getName().charAt(0) == '$') {
        genAttributeMethod(className, method, indent);
      } else if (isElement(retName)) {
        genNewElementMethod(className, method, indent);
      } else {
        genCurElementMethod(className, method, indent);
      }
    }
  }

  void genAttributeMethod(String className, Method method, int indent) {
    String methodName = method.getName();
    String attrName = methodName.substring(1).replace("__", "-");
    Type[] params = method.getGenericParameterTypes();
    echo(indent, "\n",
         "@Override\n",
         "public ", className, topMode ? " " : "<T> ", methodName, "(");
    if (params.length == 0) {
      puts(0, ") {");
      puts(indent,
           "  addAttr(\"", attrName, "\", null);\n",
           "  return this;\n", "}");
    } else if (params.length == 1) {
      String typeName = getTypeName(params[0]);
      puts(0, typeName, " value) {");
      if (typeName.equals("EnumSet<LinkType>")) {
        puts(indent,
             "  addRelAttr(\"", attrName, "\", value);\n",
             "  return this;\n", "}");
      } else if (typeName.equals("EnumSet<Media>")) {
        puts(indent,
             "  addMediaAttr(\"", attrName, "\", value);\n",
             "  return this;\n", "}");
      } else {
        puts(indent,
             "  addAttr(\"", attrName, "\", value);\n",
             "  return this;\n", "}");
      }
    } else {
      throwUnhandled(className, method);
    }
  }

  String getTypeName(Type type) {
    if (type instanceof Class<?>) {
      return ((Class<?>)type).getSimpleName();
    }
    ParameterizedType pt = (ParameterizedType) type;
    return ((Class<?>)pt.getRawType()).getSimpleName() +"<"+
        ((Class<?>)pt.getActualTypeArguments()[0]).getSimpleName() +">";
  }

  void genFactoryMethod(String retName, String methodName, int indent) {
    puts(indent, "\n",
         "private <T extends __> ", retName, "<T> ", methodName,
         "__(T e, boolean inline) {\n",
         "  return new ", retName, "<T>(\"", StringUtils.toLowerCase(retName),
         "\", e, opt(", !endTagOptional.contains(retName), ", inline, ",
         retName.equals("PRE"), ")); }");
  }

  void genNewElementMethod(String className, Method method, int indent) {
    String methodName = method.getName();
    String retName = method.getReturnType().getSimpleName();
    Class<?>[] params = method.getParameterTypes();
    echo(indent, "\n",
         "@Override\n",
         "public ", retName, "<", className, topMode ? "> " : "<T>> ",
         methodName, "(");
    if (params.length == 0) {
      puts(0, ") {");
      puts(indent,
           topMode ? "" : "  closeAttrs();\n",
           "  return ", StringUtils.toLowerCase(retName), "__" + "(this, ",
           isInline(className, retName), ");\n", "}");
    } else if (params.length == 1) {
      puts(0, "String selector) {");
      puts(indent,
           "  return setSelector(", methodName, "(), selector);\n", "}");
    } else {
      throwUnhandled(className, method);
    }
  }

  boolean isInline(String container, String className) {
    if ((container.equals("BODY") || container.equals(hamlet) ||
         container.equals("HEAD") || container.equals("HTML")) &&
        (className.equals("INS") || className.equals("DEL") ||
         className.equals("SCRIPT"))) {
      return false;
    }
    return inlineElements.contains(className);
  }

  void genCurElementMethod(String className, Method method, int indent) {
    String methodName = method.getName();
    Class<?>[] params = method.getParameterTypes();
    if (topMode || params.length > 0) {
      echo(indent, "\n",
         "@Override\n",
         "public ", className, topMode ? " " : "<T> ", methodName, "(");
    }
    if (params.length == 0) {
      if (topMode) {
        puts(0, ") {");
        puts(indent, "  return this;\n", "}");
      }
    } else if (params.length == 1) {
      if (methodName.equals("base")) {
        puts(0, "String href) {");
        puts(indent,
             "  return base().$href(href).__();\n", "}");
      } else if (methodName.equals("script")) {
        puts(0, "String src) {");
        puts(indent,
             "  return setScriptSrc(script(), src).__();\n", "}");
      } else if (methodName.equals("style")) {
        puts(0, "Object... lines) {");
        puts(indent,
             "  return style().$type(\"text/css\").__(lines).__();\n", "}");
      } else if (methodName.equals("img")) {
        puts(0, "String src) {");
        puts(indent,
             "  return ", methodName, "().$src(src).__();\n", "}");
      } else if (methodName.equals("br") || methodName.equals("hr") ||
                 methodName.equals("col")) {
        puts(0, "String selector) {");
        puts(indent,
             "  return setSelector(", methodName, "(), selector).__();\n", "}");
      }  else if (methodName.equals("link")) {
        puts(0, "String href) {");
        puts(indent,
             "  return setLinkHref(", methodName, "(), href).__();\n", "}");
      } else if (methodName.equals("__")) {
        if (params[0].getSimpleName().equals("Class")) {
          puts(0, "Class<? extends SubView> cls) {");
          puts(indent,
               "  ", topMode ? "subView" : "_v", "(cls);\n",
               "  return this;\n", "}");
        } else {
          puts(0, "Object... lines) {");
          puts(indent,
               "  _p(", needsEscaping(className), ", lines);\n",
               "  return this;\n", "}");
        }
      } else if (methodName.equals("_r")) {
        puts(0, "Object... lines) {");
        puts(indent,
             "  _p(false, lines);\n",
             "  return this;\n", "}");
      } else {
        puts(0, "String cdata) {");
        puts(indent,
             "  return ", methodName, "().__(cdata).__();\n", "}");
      }
    } else if (params.length == 2) {
      if (methodName.equals("meta")) {
        puts(0, "String name, String content) {");
        puts(indent,
             "  return meta().$name(name).$content(content).__();\n", "}");
      } else if (methodName.equals("meta_http")) {
        puts(0, "String header, String content) {");
        puts(indent,
             "  return meta().$http_equiv(header).$content(content).__();\n",
             "}");
      } else if (methodName.equals("a")) {
        puts(0, "String href, String anchorText) {");
        puts(indent,
             "  return a().$href(href).__(anchorText).__();\n", "}");
      } else if (methodName.equals("bdo")) {
        puts(0, "Dir dir, String cdata) {");
        puts(indent, "  return bdo().$dir(dir).__(cdata).__();\n", "}");
      } else if (methodName.equals("label")) {
        puts(0, "String forId, String cdata) {");
        puts(indent, "  return label().$for(forId).__(cdata).__();\n", "}");
      } else if (methodName.equals("param")) {
        puts(0, "String name, String value) {");
        puts(indent,
             "  return param().$name(name).$value(value).__();\n", "}");
      } else {
        puts(0, "String selector, String cdata) {");
        puts(indent,
             "  return setSelector(", methodName,
             "(), selector).__(cdata).__();\n", "}");
      }
    } else if (params.length == 3) {
      if (methodName.equals("a")) {
        puts(0, "String selector, String href, String anchorText) {");
        puts(indent,
             "  return setSelector(a(), selector)",
             ".$href(href).__(anchorText).__();\n", "}");
      }
    } else {
      throwUnhandled(className, method);
    }
  }

  static boolean needsEscaping(String eleName) {
    return !eleName.equals("SCRIPT") && !eleName.equals("STYLE");
  }

  static void throwUnhandled(String className, Method method) {
    throw new WebAppException("Unhandled " + className + "#" + method);
  }

  void echo(int indent, Object... args) {
    String prev = null;
    for (Object o : args) {
      String s = String.valueOf(o);
      if (!s.isEmpty() && !s.equals("\n") &&
          (prev == null || prev.endsWith("\n"))) {
        indent(indent);
      }
      prev = s;
      out.print(s);
      bytes += s.length();
    }
  }

  void indent(int indent) {
    for (int i = 0; i < indent; ++i) {
      out.print("  ");
      bytes += 2;
    }
  }

  void puts(int indent, Object... args) {
    echo(indent, args);
    out.println();
    ++bytes;
  }

  boolean isElement(String s) {
    return elementRegex.matcher(s).matches();
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = new GnuParser().parse(opts, args);
    if (cmd.hasOption("help")) {
      new HelpFormatter().printHelp("Usage: hbgen [OPTIONS]", opts);
      return;
    }
    // defaults
    Class<?> specClass = HamletSpec.class;
    Class<?> implClass = HamletImpl.class;
    String outputClass = "HamletTmp";
    String outputPackage = implClass.getPackage().getName();
    if (cmd.hasOption("spec-class")) {
      specClass = Class.forName(cmd.getOptionValue("spec-class"));
    }
    if (cmd.hasOption("impl-class")) {
      implClass = Class.forName(cmd.getOptionValue("impl-class"));
    }
    if (cmd.hasOption("output-class")) {
      outputClass = cmd.getOptionValue("output-class");
    }
    if (cmd.hasOption("output-package")) {
      outputPackage = cmd.getOptionValue("output-package");
    }
    new HamletGen().generate(specClass, implClass, outputClass, outputPackage);
  }
}
