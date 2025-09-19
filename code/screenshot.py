import easyocr
import pytesseract
import cv2
import numpy as np
from PIL import Image, ImageEnhance
import os
import sys
import requests

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# -------------------------------
# Ollama Client
# -------------------------------
class OllamaClient:
    def __init__(self, host="http://192.168.1.253:11434", 
                 model="SimonPu/Qwen3-Coder:30B-Instruct_Q4_K_XL"):
        self.host = host
        self.model = model

    def generate(self, prompt):
        url = f"{self.host}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("response", "")
        except Exception as e:
            return f"❌ Ollama request failed: {e}"

# -------------------------------
# OCR Class
# -------------------------------
class CodeScreenshotOCR:
    def __init__(self):
        self.easyocr_reader = None
        
    def preprocess_for_code(self, image_path):
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image from {image_path}")
            
        pil_img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        
        enhancer = ImageEnhance.Contrast(pil_img)
        img_enhanced = enhancer.enhance(1.8)
        
        enhancer = ImageEnhance.Sharpness(img_enhanced)
        img_sharp = enhancer.enhance(2.0)
        
        img_cv = cv2.cvtColor(np.array(img_sharp), cv2.COLOR_RGB2BGR)
        
        height, width = img_cv.shape[:2]
        if height < 800:
            scale = 800 / height
            new_width = int(width * scale)
            img_cv = cv2.resize(img_cv, (new_width, 800), interpolation=cv2.INTER_CUBIC)
        
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        denoised = cv2.medianBlur(gray, 3)
        
        binary = cv2.adaptiveThreshold(
            denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
            cv2.THRESH_BINARY, 11, 2
        )
        
        return binary, gray
    
    def extract_with_easyocr(self, image_path):
        try:
            if self.easyocr_reader is None:
                print("Initializing EasyOCR...")
                self.easyocr_reader = easyocr.Reader(['en', 'ch_sim'], gpu=False)
            
            processed_img, _ = self.preprocess_for_code(image_path)
            results = self.easyocr_reader.readtext(processed_img)
            
            results = sorted(results, key=lambda x: x[0][0][1])
            
            lines = []
            for (bbox, text, confidence) in results:
                if confidence > 0.2:
                    lines.append(text)
            
            return '\n'.join(lines)
            
        except Exception as e:
            return f"EasyOCR failed: {str(e)}"
    
    def extract_with_tesseract(self, image_path):
        try:
            processed_img, gray = self.preprocess_for_code(image_path)
            
            config = r'--oem 3 --psm 4'
            
            text = pytesseract.image_to_string(processed_img, config=config, lang='eng+chi_sim')
            
            if not text.strip():
                text = pytesseract.image_to_string(gray, config=config, lang='eng+chi_sim')
            
            return text.strip()
            
        except Exception as e:
            return f"Tesseract failed: {str(e)}"
    
    def extract_code(self, image_path, method='both'):
        if not os.path.exists(image_path):
            return f"Error: File not found - {image_path}"
        
        results = {}
        
        if method in ['easyocr', 'both']:
            print("Trying EasyOCR...")
            results['easyocr'] = self.extract_with_easyocr(image_path)
        
        if method in ['tesseract', 'both']:
            print("Trying Tesseract...")
            results['tesseract'] = self.extract_with_tesseract(image_path)
        
        return results

# -------------------------------
# MAIN
# -------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python ocr_to_ollama.py <image_file>")
        sys.exit(1)

    image_path = sys.argv[1]
    base_name = os.path.splitext(os.path.basename(image_path))[0]  # e.g. test.png → test

    ocr = CodeScreenshotOCR()
    client = OllamaClient()
    
    print(f"Extracting code from screenshot: {image_path}")
    print("=" * 60)
    
    results = ocr.extract_code(image_path, method='both')
    
    for method, text in results.items():
        print(f"\n{method.upper()} RESULTS:")
        print("-" * 40)
        print(text[:500], "...\n")
        
        # Save raw OCR output
        output_file = f"{base_name}_{method}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"Saved raw OCR to {output_file}")
        
        # Send OCR output to Ollama
        if text.strip():
            prompt = f"Convert the following OCR text into clean Python code:\n\n{text}"
            response = client.generate(prompt)
            
            py_file = f"{base_name}_{method}.py"
            with open(py_file, 'w', encoding='utf-8') as f:
                f.write(response)
            print(f"✅ Ollama Python code saved to {py_file}")

    if len(results) == 2:
        easyocr_lines = len(results['easyocr'].split('\n'))
        tesseract_lines = len(results['tesseract'].split('\n'))
        
        print(f"\nCOMPARISON:")
        print(f"EasyOCR extracted {easyocr_lines} lines")
        print(f"Tesseract extracted {tesseract_lines} lines")

if __name__ == "__main__":
    main()
