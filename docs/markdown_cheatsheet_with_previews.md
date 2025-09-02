# Markdown Cheat Sheet with Previews

This file shows **raw Markdown text** first, then its **rendered preview**.


**Preview:**

Inline math: $E = mc^2$  

limit: ```$\lim_{x \to 0} \frac{\sin x}{x} = 1$``` 

$\lim_{x \to 0} \frac{\sin x}{x} = 1$

```
Block math:
```
$$
\sum_{i=1}^{n} i = \frac{n(n+1)}{2}
$$
```
$$
\sum_{i=1}^{n} i = \frac{n(n+1)}{2}
$$
```
$$
f(x) = \int_{-\infty}^\infty e^{-x^2} dx
$$
```
$$
f(x) = \int_{-\infty}^\infty e^{-x^2} dx
$$
---
$$
\sum_{i=1}^{n} i = \frac{n(n+1)}{2}
$$  
---
$$
\prod_{k=1}^{n} k = n!
$$  
---

**Preview:**

Summation:
$$
\sum_{i=1}^{n} i = \frac{n(n+1)}{2}
$$

Product:
$$
\prod_{k=1}^{n} k = n!
$$

Limit:
$$
\lim_{x \to 0} \frac{\sin x}{x} = 1
$$

Fraction:
$$
\frac{a+b}{c+d}
$$

Square root:
$$
\sqrt{x^2 + y^2}
$$

Matrix:
$$
\begin{bmatrix}
1 & 2 \\
3 & 4
\end{bmatrix}
$$

---

```
$$
P(A|B) = \frac{P(A \cap B)}{P(B)}
$$

```
$$
P(A|B) = \frac{P(A \cap B)}{P(B)}
$$
---
```
$$
E[X] = \sum_{i} x_i P(X=x_i) \quad \text{or} \quad E[X] = \int_{-\infty}^{\infty} x f_X(x) dx
$$
```
$$
E[X] = \sum_{i} x_i P(X=x_i) \quad \text{or} \quad E[X] = \int_{-\infty}^{\infty} x f_X(x) dx
$$
---
```
$$
F_X(x) = P(X \le x) = \int_{-\infty}^{x} f_X(t) dt
$$
```
$$
F_X(x) = P(X \le x) = \int_{-\infty}^{x} f_X(t) dt
$$
---
```
$$
\sigma_X = \sqrt{\text{Var}(X)}
$$
```
$$
\sigma_X = \sqrt{\text{Var}(X)}
$$
---
```
$$
f_X(x) \ge 0, \quad \int_{-\infty}^{\infty} f_X(x) dx = 1
$$
```
$$
f_X(x) \ge 0, \quad \int_{-\infty}^{\infty} f_X(x) dx = 1
$$
---


## 1. Headings
**Markdown:**
```
# Heading 1
## Heading 2
### Heading 3
#### Heading 4
##### Heading 5
###### Heading 6
```

**Preview:**
# Heading 1
## Heading 2
### Heading 3
#### Heading 4
##### Heading 5
###### Heading 6

---

## 2. Bold, Italic, Strikethrough
**Markdown:**
```
**bold**
*italic*
~~strikethrough~~
```

**Preview:**
**bold**  
*italic*  
~~strikethrough~~  

---

## 3. Blockquote
**Markdown:**
```
> This is a blockquote
>> Nested blockquote
```

**Preview:**
> This is a blockquote
>> Nested blockquote

---

## 4. Lists
**Markdown:**
```
- Item 1
- Item 2
  - Subitem 2a
  - Subitem 2b

1. First
2. Second
3. Third
```

**Preview:**
- Item 1
- Item 2
  - Subitem 2a
  - Subitem 2b

1. First
2. Second
3. Third

---

## 5. Code
**Markdown:**
```
Inline code: `print("Hello")`

Code block:
\`\`\`python
def hello():
    print("Hello World")
\`\`\`
```

**Preview:**
Inline code: `print("Hello")`

```python
def hello():
    print("Hello World")
```

---

## 6. Links and Images
**Markdown:**
```
[OpenAI](https://openai.com)

![Alt text](https://via.placeholder.com/150)
```

**Preview:**
[OpenAI](https://openai.com)  

![Alt text](https://via.placeholder.com/150)

---

## 7. Horizontal Rule
**Markdown:**
```
---
```

**Preview:**
---

---

## 8. Tables
**Markdown:**
```
| Name  | Age |
|-------|-----|
| John  | 25  |
| Alice | 30  |
```

**Preview:**
| Name  | Age |
|-------|-----|
| John  | 25  |
| Alice | 30  |

---

## 9. Task Lists
**Markdown:**
```
- [x] Task 1
- [ ] Task 2
- [ ] Task 3
```

**Preview:**
- [x] Task 1
- [ ] Task 2
- [ ] Task 3

---

## 10. Escaping Characters
**Markdown:**
```
\*This is not italic\*
```

**Preview:**
\*This is not italic\*

---

## 11. Footnotes
**Markdown:**
```
Here is a footnote reference[^1].

[^1]: This is the footnote.
```

**Preview:**
Here is a footnote reference[^1].

[^1]: This is the footnote.

---

## 12. HTML in Markdown
**Markdown:**
```
<span style="color:red">This text is red (works in some renderers)</span>
```

**Preview:**
<span style="color:red">This text is red (works in some renderers)</span>

---

## Note on Colors
Standard Markdown **does not support colors**.  
You can use inline **HTML** (like `<span style="color:red">`) but support depends on the renderer (e.g., GitHub supports it).



---

## 13. Colored Text (via HTML)
**Markdown:**
```
<span style="color:red">Red Text</span>  
<span style="color:blue">Blue Text</span>  
<span style="color:green">Green Text</span>  
<span style="color:purple">Purple Text</span>  
<span style="color:orange">Orange Text</span>
```

**Preview:**
<span style="color:red">Red Text</span>  
<span style="color:blue">Blue Text</span>  
<span style="color:green">Green Text</span>  
<span style="color:purple">Purple Text</span>  
<span style="color:orange">Orange Text</span>

---

## 14. Colored Backgrounds (via HTML)
**Markdown:**
```
<span style="background-color:yellow">Yellow Background</span>  
<span style="background-color:lightblue">Light Blue Background</span>  
<span style="background-color:pink">Pink Background</span>
```

**Preview:**
<span style="background-color:yellow">Yellow Background</span>  
<span style="background-color:lightblue">Light Blue Background</span>  
<span style="background-color:pink">Pink Background</span>

---

⚠️ Note: Colored text and backgrounds work only in renderers that allow inline HTML (e.g., GitHub, some Markdown editors).  


---

## 15. Highlighting Styles with CSS (Advanced)
You can define reusable styles inside a `<style>` block (works in some Markdown renderers that allow embedded HTML).

**Markdown:**
```
<style>
.red-text { color: red; }
.green-bg { background-color: lightgreen; }
.highlight { background-color: yellow; font-weight: bold; }
</style>

<span class="red-text">This is red using CSS class</span>  
<span class="green-bg">This has a green background</span>  
<span class="highlight">This is highlighted text</span>
```

**Preview:**
<style>
.red-text { color: red; }
.green-bg { background-color: lightgreen; }
.highlight { background-color: yellow; font-weight: bold; }
</style>

<span class="red-text">This is red using CSS class</span>  
<span class="green-bg">This has a green background</span>  
<span class="highlight">This is highlighted text</span>

---

⚠️ Note: CSS blocks only work in Markdown renderers that allow raw HTML + CSS (not supported in GitHub Markdown, but works in some documentation tools and static site generators).

