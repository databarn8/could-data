# Markdown Cheat Sheet

### Heading

# H1
## H2             
### H3            
**bold text**     
*italicized text* 

### Blockquote

> blockquote

### Ordered List

1. First item <br>
    a. what is one item 1st  
    b. what is one item 2nd  
    c. what is one item 3rd
2. Second item <br>
    a. what is one item Tim
    b. what is one item John
    c. what is one item Hello
3. Third item

### Unordered List

- First item
  - First child 1
  - First child 2
  - third child 3
- Second item
- Third item

### Code

`code`

### Horizontal Rule

---

### Link

[Markdown Guide](https://www.markdownguide.org)

### Image

![alt text](https://www.markdownguide.org/assets/images/tux.png)

## Extended Syntax

These elements extend the basic syntax by adding additional features. Not all Markdown applications support these elements.

### Table

| Syntax | Description |
| ----------- | ----------- |
| Header | Title |
| Paragraph | Text |

### Fenced Code Block

```
{
  "firstName": "John",
  "lastName": "Smith",
  "age": 25
}
```

### Footnote

Here's a sentence with a footnote. [^1]

[^1]: This is the footnote.

### Heading ID

### My Great Heading {#custom-id}

### Definition List

term
: definition

### Strikethrough

~~The world is flat.~~

### Task List

- [x] Write the press release
- [ ] Update the website
- [ ] Contact the media

### Emoji

That is so funny! :joy:

## Code blcok

```python 
def hello(): 
  print("Hello, world!") 
``` 

```sql
SELECT * FROM customers WHERE id = 1;
```

(See also [Copying and Pasting Emoji](https://www.markdownguide.org/extended-syntax/#copying-and-pasting-emoji))

### Highlight

I need to highlight these ==very important words==.

### Subscript

H~2~O

### Superscript

X^2^


**very important**  // bold <br>
*very important*    // italic <br>
H<sub>2</sub>O     <br>
X<sup>2</sup>      <br>

-----------------
Inline: $\sum_{i=1}^n x_i$

Block:
$$
\sum_{i=1}^n x_i
$$
---
Inline: $\binom{n}{k}$

Block:
$$
\binom{n}{k}
$$
---
Inline: $\frac{a}{b}$

Block:
$$
\frac{a}{b}
$$
---
Inline: $\sqrt{x}$
Inline: $\sqrt[3]{x}$  
Inline: $\sqrt[4]{y}$ 

Block:
$$
\sqrt{x}
$$
---
Inline: $\alpha, \beta, \gamma, \pi, \theta$

Block:
$$
\alpha, \beta, \gamma, \pi, \theta
$$

---
Inline: $\int_0^1 x^2 \, dx$

Block:
$$
\int_0^1 x^2 \, dx
$$
---
Inline: $\lim_{n \to \infty} \frac{1}{n}$

Block:
$$
\lim_{n \to \infty} \frac{1}{n}
$$

Inline: $\frac{dy}{dx}$

Block:
$$
\frac{dy}{dx}
$$
Inline: $\frac{d^2y}{dx^2}$

Block:
$$
\frac{d^2y}{dx^2}
$$

2. Block of code (fenced code block)

Use three backticks before and after your code:


```python 
def hello(): 
print("Hello, world!") 
``` 
