"""
LLM Tagger module for the File Indexer.
Uses llama-cpp-python to generate metadata from text and image files.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Try to import llama-cpp-python
try:
    from llama_cpp import Llama
except ImportError:
    Llama = None
    print("Warning: llama-cpp-python not installed. Install with: pip install llama-cpp-python")

# Try to import PIL for image processing
try:
    from PIL import Image
except ImportError:
    Image = None
    print("Warning: Pillow not installed. Install with: pip install Pillow")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LLMTagger:
    """
    LLM-based tagger that generates metadata for text and image files.
    Uses local GGUF models for inference (text and vision).
    """
    
    # Default JSON schema for the output
    DEFAULT_RESPONSE = {
        "summary": "Could not parse LLM response",
        "document_type": "other",
        "tags": [],
        "keywords": [],
        "date_hint": None,
        "people_mentioned": []
    }
    
    def __init__(
        self, 
        model_path: str, 
        vision_model_path: Optional[str] = None,
        use_gpu: bool = False, 
        gpu_layers: int = 0
    ):
        """
        Initialize the LLM tagger.
        
        Args:
            model_path: Path to the text .gguf model file
            vision_model_path: Optional path to vision .gguf model file
            use_gpu: Whether to use GPU acceleration
            gpu_layers: Number of layers to offload to GPU (0 = CPU only)
        """
        self.model_path = Path(model_path)
        self.vision_model_path = Path(vision_model_path) if vision_model_path else None
        self.use_gpu = use_gpu
        self.gpu_layers = gpu_layers if use_gpu else 0
        self.model = None
        self.vision_model = None
        
        # Load models
        self._load_model()
        if self.vision_model_path:
            self._load_vision_model()
    
    def _load_model(self) -> None:
        """
        Load the text LLM model using llama-cpp-python.
        """
        if Llama is None:
            logger.error("llama-cpp-python is not installed")
            return
        
        if not self.model_path.exists():
            logger.error(f"Model file not found: {self.model_path}")
            return
        
        try:
            logger.info(f"Loading text model from {self.model_path}")
            logger.info(f"GPU: {self.use_gpu}, Layers: {self.gpu_layers}")
            
            self.model = Llama(
                model_path=str(self.model_path),
                n_ctx=2048,  # Context window size
                n_threads=4,  # Number of CPU threads
                n_gpu_layers=self.gpu_layers,
                verbose=False,  # Reduce console output
                use_mlock=False,  # Avoid memory locking issues
                use_mmap=True,  # Memory map the model for faster loading
            )
            logger.info("Text model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load text model: {e}")
            self.model = None
    
    def _load_vision_model(self) -> None:
        """
        Load the vision-language model for image understanding.
        """
        if Llama is None:
            logger.error("llama-cpp-python is not installed, cannot load vision model")
            return
        
        if not self.vision_model_path or not self.vision_model_path.exists():
            logger.error(f"Vision model file not found: {self.vision_model_path}")
            return
        
        try:
            logger.info(f"Loading vision model from {self.vision_model_path}")
            logger.info(f"GPU: {self.use_gpu}, Layers: {self.gpu_layers}")
            
            # Vision models need a chat format for multimodal input
            self.vision_model = Llama(
                model_path=str(self.vision_model_path),
                n_ctx=2048,
                n_threads=4,
                n_gpu_layers=self.gpu_layers,
                verbose=False,
                use_mlock=False,
                use_mmap=True,
                # Vision-specific settings
                chat_format="llava-1.5",  # Common format for vision models
            )
            logger.info("Vision model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load vision model: {e}")
            self.vision_model = None
    
    def is_loaded(self) -> bool:
        """
        Check if the text model is loaded and ready.
        
        Returns:
            True if model is loaded, False otherwise
        """
        return self.model is not None
    
    def is_vision_loaded(self) -> bool:
        """
        Check if the vision model is loaded and ready.
        
        Returns:
            True if vision model is loaded, False otherwise
        """
        return self.vision_model is not None
    
    def _build_prompt(self, file_path: str, extracted_text: str) -> str:
        """
        Build the prompt for the text LLM.
        
        Args:
            file_path: Path to the file (used for context)
            extracted_text: Extracted text content
            
        Returns:
            Prompt string for the LLM
        """
        # Truncate text if it's too long (model has n_ctx=2048 tokens)
        # Rough estimate: 1 token ≈ 4 characters for English
        max_chars = 4000  # Safe limit for 2048 tokens
        if len(extracted_text) > max_chars:
            extracted_text = extracted_text[:max_chars] + "\n...[truncated]"
            logger.debug(f"Truncated text to {max_chars} characters")
        
        # Get filename for context
        filename = Path(file_path).name
        
        prompt = f"""You are a document metadata generator. Analyze the following file content and return ONLY a valid JSON object with the specified fields.

Filename: {filename}

File content: {extracted_text} 


Required JSON format:
{{
  "summary": "One or two sentence plain English description (max 200 chars)",
  "document_type": "One of: invoice|resume|passport|certificate|photo|report|contract|notes|spreadsheet|code|personal|other",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"],
  "keywords": ["word1", "word2", "word3", "word4", "word5", "word6", "word7", "word8", "word9", "word10", "word11", "word12"],
  "date_hint": "YYYY or YYYY-MM or null",
  "people_mentioned": ["Person Name 1", "Person Name 2"]
}}

Rules:
1. Return ONLY raw JSON - no markdown code blocks, no explanation, no extra text
2. Summary must be concise (max 200 characters)
3. Tags: 3-8 short descriptive tags
4. Keywords: 5-12 important words/phrases from the content
5. Date hint: Use YYYY (e.g., "2024") or YYYY-MM (e.g., "2024-03") format, or null if no date found
6. People mentioned: Extract full names when possible, use empty list if none
7. For document_type, choose the best fit from the list provided

JSON:"""
        
        return prompt
    
    def _build_vision_prompt(self, image_path: str) -> str:
        """
        Build the prompt for the vision LLM.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Prompt string for the vision model
        """
        filename = Path(image_path).name
        
        prompt = f"""You are analyzing an image file named '{filename}'. 

Please describe this image and return ONLY a valid JSON object with the following fields:

{{
  "summary": "A brief description of what this image shows (max 200 chars)",
  "document_type": "One of: photo|passport|certificate|receipt|screenshot|diagram|other",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
  "keywords": ["word1", "word2", "word3", "word4", "word5"],
  "date_hint": "YYYY or YYYY-MM or null if no date visible",
  "people_mentioned": ["names of any people visible, or empty list"]
}}

Rules:
1. Return ONLY raw JSON - no markdown code blocks, no explanation
2. Summary: describe the main subject, objects, and context
3. Document type: photo for general photos, passport for ID documents, receipt for receipts, etc.
4. Tags: descriptive tags about content (e.g., "landscape", "portrait", "document", "screenshot")
5. Keywords: important visible text or objects
6. Date hint: if a date is visible in the image (sign, timestamp, etc.)
7. People mentioned: names from visible text or recognized individuals

JSON:"""
        
        return prompt
    
    def _parse_response(self, response_text: str, file_path: str, is_image: bool = False) -> Dict[str, Any]:
        """
        Parse the LLM's response and extract JSON.
        
        Args:
            response_text: Raw text from LLM
            file_path: Path to file (for error context)
            is_image: Whether this is from an image file
            
        Returns:
            Parsed JSON dictionary
        """
        # Clean the response - remove any markdown code blocks
        cleaned = response_text.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]  # Remove ```json
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]  # Remove ```
        
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]  # Remove closing ```
        
        cleaned = cleaned.strip()
        
        # Try to find JSON object in the response (in case there's extra text)
        start_idx = cleaned.find("{")
        end_idx = cleaned.rfind("}") + 1
        
        if start_idx != -1 and end_idx > start_idx:
            json_str = cleaned[start_idx:end_idx]
        else:
            json_str = cleaned
        
        # Parse JSON
        try:
            result = json.loads(json_str)
            
            # Validate and ensure all required fields exist
            validated_result = self.DEFAULT_RESPONSE.copy()
            validated_result.update(result)
            
            # Ensure specific field types
            if not isinstance(validated_result["summary"], str):
                validated_result["summary"] = str(validated_result["summary"])[:200]
            
            if not isinstance(validated_result["document_type"], str):
                validated_result["document_type"] = "photo" if is_image else "other"
            
            if not isinstance(validated_result["tags"], list):
                validated_result["tags"] = []
            
            if not isinstance(validated_result["keywords"], list):
                validated_result["keywords"] = []
            
            # Convert date_hint to string or None
            if validated_result["date_hint"] is not None:
                validated_result["date_hint"] = str(validated_result["date_hint"])
            
            if not isinstance(validated_result["people_mentioned"], list):
                validated_result["people_mentioned"] = []
            
            # Add filename as a keyword if keywords list is empty
            if not validated_result["keywords"]:
                validated_result["keywords"] = [Path(file_path).stem]
            
            logger.debug(f"Successfully parsed response: {validated_result}")
            return validated_result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Raw response: {response_text}")
            
            # Return fallback with filename
            fallback = self.DEFAULT_RESPONSE.copy()
            filename = Path(file_path).stem
            fallback["summary"] = f"Unable to process: {filename}"
            fallback["keywords"] = [filename]
            if is_image:
                fallback["document_type"] = "photo"
            return fallback
    
    def tag_text_file(self, file_path: str, extracted_text: str) -> Dict[str, Any]:
        """
        Generate metadata for a text file using the LLM.
        
        Args:
            file_path: Path to the file (used for context)
            extracted_text: Extracted text content from the file
            
        Returns:
            Dictionary containing metadata
        """
        # Route image files to vision model
        if extracted_text == "IMAGE_FILE":
            return self.tag_image_file(file_path)
        
        if not self.is_loaded():
            logger.error("Text model not loaded, cannot tag file")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "Model not available"
            return fallback
        
        if not extracted_text:
            logger.warning(f"Empty content for {file_path}")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "No text content available"
            filename = Path(file_path).stem
            fallback["keywords"] = [filename]
            return fallback
        
        try:
            # Build prompt
            prompt = self._build_prompt(file_path, extracted_text)
            
            # Generate response from LLM
            logger.info(f"Tagging text file: {Path(file_path).name}")
            
            response = self.model(
                prompt=prompt,
                max_tokens=512,  # Enough for JSON output
                temperature=0.1,  # Low temperature for consistent output
                top_p=0.95,
                stop=["```", "}"],  # Stop tokens - we'll add back the closing brace
                echo=False,
                frequency_penalty=0.0,
                presence_penalty=0.0
            )
            
            # Extract the generated text
            generated_text = response['choices'][0]['text'].strip()
            
            # Add back closing brace if it was cut off
            if generated_text and not generated_text.endswith('}'):
                # Count opening braces
                open_braces = generated_text.count('{')
                close_braces = generated_text.count('}')
                if open_braces > close_braces:
                    generated_text += '}' * (open_braces - close_braces)
            
            logger.debug(f"Generated response: {generated_text[:200]}...")
            
            # Parse the response
            result = self._parse_response(generated_text, file_path, is_image=False)
            
            # Add file metadata
            result["file_name"] = Path(file_path).name
            result["file_path"] = file_path
            
            logger.info(f"Successfully tagged {Path(file_path).name}: "
                       f"Type={result['document_type']}, "
                       f"Tags={len(result['tags'])}, "
                       f"Keywords={len(result['keywords'])}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tagging file {file_path}: {e}", exc_info=True)
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = f"Error during processing: {str(e)[:100]}"
            filename = Path(file_path).stem
            fallback["keywords"] = [filename]
            return fallback
    
    def tag_image_file(self, file_path: str) -> Dict[str, Any]:
        """
        Generate metadata for an image file using the vision-language model.
        
        Args:
            file_path: Path to the image file
            
        Returns:
            Dictionary containing metadata
        """
        if not self.is_vision_loaded():
            logger.warning(f"Vision model not loaded for image: {file_path}")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "Image file - vision model not configured"
            fallback["document_type"] = "photo"
            fallback["keywords"] = [Path(file_path).stem]
            return fallback
        
        if Image is None:
            logger.error("Pillow not installed, cannot process image")
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = "Image processing library not available"
            fallback["document_type"] = "photo"
            return fallback
        
        try:
            # Load and preprocess the image
            logger.info(f"Processing image: {Path(file_path).name}")
            
            with Image.open(file_path) as img:
                # Convert RGBA to RGB if necessary
                if img.mode in ('RGBA', 'LA', 'P'):
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    rgb_img.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = rgb_img
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Resize to max 512x512 while maintaining aspect ratio
                img.thumbnail((512, 512), Image.Resampling.LANCZOS)
                
                # Save to temporary file or bytes for the model
                # llama-cpp-python expects a file path for vision models
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_file:
                    img.save(tmp_file, 'JPEG', quality=85)
                    tmp_path = tmp_file.name
            
            # Build prompt for vision model
            prompt = self._build_vision_prompt(file_path)
            
            # Use the vision model with image
            # Vision models use the create_chat_completion method with images
            response = self.vision_model.create_chat_completion(
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": f"file://{tmp_path}"}}
                        ]
                    }
                ],
                max_tokens=512,
                temperature=0.1,
                top_p=0.95,
                stop=["```", "}"],
            )
            
            # Clean up temporary file
            Path(tmp_path).unlink()
            
            # Extract the generated text
            generated_text = response['choices'][0]['message']['content'].strip()
            
            # Add back closing brace if it was cut off
            if generated_text and not generated_text.endswith('}'):
                open_braces = generated_text.count('{')
                close_braces = generated_text.count('}')
                if open_braces > close_braces:
                    generated_text += '}' * (open_braces - close_braces)
            
            logger.debug(f"Vision model response: {generated_text[:200]}...")
            
            # Parse the response
            result = self._parse_response(generated_text, file_path, is_image=True)
            
            # Add file metadata
            result["file_name"] = Path(file_path).name
            result["file_path"] = file_path
            
            logger.info(f"Successfully tagged image {Path(file_path).name}: "
                       f"Type={result['document_type']}, "
                       f"Tags={len(result['tags'])}, "
                       f"Keywords={len(result['keywords'])}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error tagging image {file_path}: {e}", exc_info=True)
            fallback = self.DEFAULT_RESPONSE.copy()
            fallback["summary"] = f"Error processing image: {str(e)[:100]}"
            fallback["document_type"] = "photo"
            filename = Path(file_path).stem
            fallback["keywords"] = [filename]
            return fallback
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get information about the loaded models.
        
        Returns:
            Dictionary with model information
        """
        info = {
            "text_model_loaded": self.is_loaded(),
            "vision_model_loaded": self.is_vision_loaded(),
            "text_model_path": str(self.model_path) if self.model_path else None,
            "vision_model_path": str(self.vision_model_path) if self.vision_model_path else None,
            "use_gpu": self.use_gpu,
            "gpu_layers": self.gpu_layers,
            "context_size": 2048
        }
        return info


def main():
    """Test the LLM tagger with sample text or images."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test LLM tagger for text and images")
    parser.add_argument("model_path", help="Path to text GGUF model file")
    parser.add_argument("--vision-model", "-v", help="Path to vision GGUF model file (for images)")
    parser.add_argument("--file", "-f", help="Path to file to tag (text or image)")
    parser.add_argument("--text", "-t", help="Direct text to tag (optional)")
    parser.add_argument("--image", "-i", help="Path to image file to tag")
    parser.add_argument("--gpu", action="store_true", help="Use GPU acceleration")
    parser.add_argument("--gpu-layers", type=int, default=0, help="Number of GPU layers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Determine what to tag
    is_image = False
    content = None
    file_path = None
    
    if args.image:
        # Tag an image file
        is_image = True
        file_path = args.image
        if not Path(file_path).exists():
            print(f"Error: Image file not found: {file_path}")
            sys.exit(1)
        print(f"Processing image: {file_path}")
        
    elif args.file:
        # Tag a file (could be text or image)
        file_path = args.file
        ext = Path(file_path).suffix.lower()
        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
            is_image = True
            print(f"Processing image: {file_path}")
        else:
            # Read text file
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            print(f"Loaded text file: {file_path}")
            
    elif args.text:
        # Use direct text
        content = args.text
        file_path = "direct_input.txt"
        print("Using direct text input...")
        
    else:
        # Use sample text
        sample_text = """
        INVOICE
        Date: 2024-03-15
        Invoice #: INV-2024-001
        Bill To: John Smith, 123 Main St, Anytown, USA
        Items:
        - 2x Laptop Stand @ $45.00 = $90.00
        - 1x Wireless Mouse @ $25.00 = $25.00
        Subtotal: $115.00
        Tax (10%): $11.50
        Total: $126.50
        Thank you for your business!
        """
        content = sample_text
        file_path = "sample_invoice.txt"
        print("Using sample invoice text...")
    
    # Initialize tagger
    print(f"\nLoading text model from: {args.model_path}")
    if args.vision_model:
        print(f"Loading vision model from: {args.vision_model}")
    print(f"GPU: {args.gpu}, GPU Layers: {args.gpu_layers}")
    
    tagger = LLMTagger(
        model_path=args.model_path,
        vision_model_path=args.vision_model,
        use_gpu=args.gpu,
        gpu_layers=args.gpu_layers
    )
    
    if not tagger.is_loaded():
        print("❌ Failed to load text model")
        sys.exit(1)
    
    if is_image and not tagger.is_vision_loaded() and args.vision_model:
        print("❌ Failed to load vision model")
        sys.exit(1)
    
    print("✅ Models loaded successfully")
    print("\n" + "=" * 60)
    print("TAGGING FILE...")
    print("=" * 60)
    
    # Tag the content
    if is_image:
        result = tagger.tag_image_file(file_path)
    else:
        result = tagger.tag_text_file(file_path, content)
    
    # Display results
    print("\n📋 METADATA RESULTS:")
    print("-" * 60)
    print(f"Summary: {result['summary']}")
    print(f"Document Type: {result['document_type']}")
    print(f"Tags: {', '.join(result['tags'])}")
    print(f"Keywords: {', '.join(result['keywords'][:10])}")  # Show first 10
    print(f"Date Hint: {result['date_hint']}")
    print(f"People Mentioned: {', '.join(result['people_mentioned']) if result['people_mentioned'] else 'None'}")
    print("-" * 60)
    
    # Pretty print full JSON
    print("\n📄 FULL JSON OUTPUT:")
    output_json = {k: v for k, v in result.items() if k not in ['file_path', 'file_name']}
    print(json.dumps(output_json, indent=2, ensure_ascii=False))
    
    # Show model info
    print("\n🔧 MODEL INFO:")
    info = tagger.get_model_info()
    for key, value in info.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()


