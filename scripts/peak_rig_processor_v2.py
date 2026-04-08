#!/usr/bin/env python3
"""
PEAKATS Resume Intelligence Gateway (RIG) - GEMINI VERSION v3.1
WITH SMART UNMATCHED RESUME HANDLING
NOW WRITES DIRECTLY TO SUPABASE

Features:
- Gemini 3 Flash API for resume parsing
- Enhanced matching (exact, fuzzy, first-name-only)
- Unmatched resume review report
- Suggested fixes for manual review
- Direct Supabase integration

RWP SCORING v2.1 (Updated Feb 28, 2026):
- 11 FEDEX_ACTIVE: Current FedEx Driver
- 10 FEDEX_FORMER: Past FedEx Driver OR FedEx Handler
- 9 DELIVERY_EXP: UPS, Amazon, USPS, DHL drivers
- 7 WAREHOUSE_EXP: Warehouse, dock, package sorter
- 6 COMMERCIAL_DRIVER: CDL, box truck, commercial
- 3 LOW_RELEVANCE: Gig only (DoorDash, Uber Eats)
- 1 UNWEIGHTED: No experience (0/null = never reviewed)
"""

import os
import json
import base64
import re
import time
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, List
from difflib import SequenceMatcher
from sqlalchemy import create_engine, text
from docx import Document as DocxDocument
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.enums import TA_LEFT

# Database URL
DB_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# Google Gemini API import
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("ERROR: google-generativeai package not installed")
    print("Install with: pip install google-generativeai --break-system-packages")
    raise

# =============================================================================
# RWP SYSTEM PROMPT v2.1 - UPDATED FEB 28, 2026
# =============================================================================
RIG_SYSTEM_PROMPT = """ROLE: PEAK Recruiting Data Operations Agent

TASK: Process the attached resume file. Extract candidate contact data and assess driving experience.

PART 1: DATA EXTRACTION RULES
1. Source File: Capture exact filename
2. Name: Extract full name, split into First/Last, Title Case
3. Phone: 10-digit string, no formatting, remove leading '1', use '0000000000' if missing
4. Email: Lowercase, use NULL if missing

PART 2: RWP SCORING RULES (Assign exactly one score)

Score 11 - FEDEX_ACTIVE:
- FedEx DRIVING role that is CURRENT
- Employment shows "Present", "Current", or current year (2025, 2026)
- Job titles: FedEx Driver, FedEx Courier, Delivery Driver at FedEx Ground/Express
- MUST be actively employed in a DRIVING capacity

Score 10 - FEDEX_FORMER:
- FedEx DRIVING role that has ENDED (past employment with end date), OR
- FedEx Package Handler (current or former, any dates)
- FedEx warehouse roles count as 10

Score 9 - DELIVERY_EXP:
- Professional delivery/courier experience (NOT FedEx driving)
- UPS Driver, Amazon DSP/Flex, USPS Mail Carrier, DHL Courier
- OnTrac, LaserShip, furniture delivery, appliance delivery
- Must be actual delivery DRIVING roles

Score 7 - WAREHOUSE_EXP:
- Warehouse/dock experience with packages (no driving routes)
- Amazon warehouse, UPS facility, distribution center
- Dock worker, loading dock, package sorter
- Fulfillment center, shipping/receiving
- Knows packages but needs driving training

Score 6 - COMMERCIAL_DRIVER:
- Professional commercial driving (no parcel/delivery focus)
- CDL-A or CDL-B truck driver (OTR, regional)
- Box truck driver (non-delivery), bus driver
- Commercial fleet, shuttle, limousine
- Knows driving but needs package pace training

Score 3 - LOW_RELEVANCE:
- Minimal or gig-economy only experience
- Uber/Lyft driver, DoorDash, Uber Eats, GrubHub
- Instacart, pizza delivery, personal vehicle only
- "Driver" mentioned but minimal details

Score 1 - UNWEIGHTED:
- No professional driving or logistics experience
- Office/administrative, retail/restaurant (non-delivery)
- No work history provided, resume cannot be parsed

IMPORTANT SCORING RULES:
- 11 = FedEx DRIVER + currently employed (shows "Present" or current year)
- 10 = FedEx DRIVER + employment ended, OR any FedEx Handler/Warehouse
- 9 = Delivery DRIVING experience that is NOT FedEx
- 7 = Warehouse/dock work (no driving)
- 6 = Commercial driving (no delivery/parcel focus)
- Multiple experience = use HIGHEST applicable tier
- FedEx experience always trumps other experience

PART 3: OUTPUT FORMAT
Return valid JSON only:
{
    "source_file": "filename.pdf",
    "first_name": "John",
    "last_name": "Doe",
    "phone": "4045551234",
    "email": "john.doe@email.com",
    "rwp_score": 11,
    "rwp_classification": "FEDEX_ACTIVE",
    "rationale": "FedEx Ground Driver 2023-Present, currently employed"
}

CRITICAL: Rationale must be max 15 words."""


class ResumeProcessor:
    def __init__(self, api_key: str = None, create_unmatched: bool = False):
        self.api_key = api_key or os.environ.get('GEMINI_API_KEY')
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment")
        
        # Configure Gemini
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('models/gemini-3-flash')
        
        self.engine = None
        self.create_unmatched = create_unmatched
        
        # Track unmatched for review report
        self.unmatched_resumes = []
        
        self.stats = {
            'resumes_found': 0,
            'matched_exact': 0,
            'matched_fuzzy': 0,
            'matched_first_name': 0,
            'unmatched': 0,
            'created': 0,
            'api_calls': 0,
            'api_errors': 0,
            'score_distribution': {
                '11': 0,
                '10': 0,
                '9': 0,
                '7': 0,
                '6': 0,
                '3': 0,
                '1': 0
            }
        }
    
    def connect_db(self):
        """Establish database connection to Supabase"""
        self.engine = create_engine(DB_URL)
    
    def close_db(self):
        """Close database connection"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    def create_candidate_from_resume(self, resume_data: Dict, client_id: str, resume_path: Path) -> bool:
        """
        Create a new candidate record from resume data (for unmatched resumes)
        Returns: True if created successfully
        """
        try:
            with self.engine.connect() as conn:
                # Check if candidate already exists (by name + client)
                existing = conn.execute(text("""
                    SELECT id FROM candidates 
                    WHERE LOWER(first_name) = LOWER(:first_name)
                    AND LOWER(last_name) = LOWER(:last_name)
                    AND client_id = :client_id
                """), {
                    "first_name": resume_data['first_name'],
                    "last_name": resume_data['last_name'],
                    "client_id": client_id
                }).fetchone()
                
                if existing:
                    print(f"  ⚠️  Candidate already exists (ID: {existing[0]})")
                    return False
                
                # Status at insert is always Intake -- scorer does not set Active
                # FADV submission is the only thing that sets Active
                rwp_score = resume_data['rwp_score']
                new_status = 'Intake'

                conn.execute(text("""
                    INSERT INTO candidates (
                        client_id, first_name, last_name,
                        phone, email,
                        rwp_score, rwp_classification, rwp_rationale,
                        resume_filename, status, tag,
                        import_source, source_channel,
                        reject_reason,
                        created_at, updated_at
                    ) VALUES (
                        :client_id, :first_name, :last_name,
                        :phone, :email,
                        :rwp_score, :rwp_classification, :rwp_rationale,
                        :resume_filename, :status, 'Driver',
                        'resume_direct', 'resume_direct',
                        :reject_reason,
                        NOW(), NOW()
                    )
                """), {
                    "client_id": client_id,
                    "first_name": resume_data['first_name'],
                    "last_name": resume_data['last_name'],
                    "phone": resume_data.get('phone', ''),
                    "email": resume_data.get('email') if resume_data.get('email') != 'NULL' else None,
                    "rwp_score": rwp_score,
                    "rwp_classification": resume_data['rwp_classification'],
                    "rwp_rationale": resume_data.get('rationale', ''),
                    "resume_filename": resume_path.name,
                    "status": new_status,
                    "reject_reason": 'low_rwp' if new_status == 'Rejected' else None,
                })
                conn.commit()

                self.stats['created'] += 1
                print(f"  ➕ CREATED new candidate ({new_status}): {resume_data['first_name']} {resume_data['last_name']}")
                return True
                
        except Exception as e:
            print(f"  ❌ Failed to create candidate: {e}")
            return False
    
    def parse_resume_with_gemini(self, resume_path: Path) -> Optional[Dict]:
        """
        Send resume to Gemini API for parsing
        Returns: Dict with candidate data + RWP scoring
        """
        try:
            # Read PDF as base64
            with open(resume_path, 'rb') as f:
                pdf_data = base64.b64encode(f.read()).decode('utf-8')
            
            # Rate limiting (15 requests per minute)
            time.sleep(4)
            
            # Prepare PDF part for Gemini
            pdf_part = {
                'mime_type': 'application/pdf',
                'data': pdf_data
            }
            
            # Call Gemini API
            prompt = f"{RIG_SYSTEM_PROMPT}\n\nAnalyze this resume and return the JSON response."
            
            response = self.model.generate_content([prompt, pdf_part])
            self.stats['api_calls'] += 1
            
            # Extract JSON from response
            response_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0].strip()
            
            # Parse JSON
            result = json.loads(response_text)
            
            # Validate required fields
            required_fields = ['first_name', 'last_name', 'phone', 'rwp_score', 'rwp_classification']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Normalize score to int
            result['rwp_score'] = int(float(result['rwp_score']))
            
            # RWP v2.1: UNWEIGHTED = 1 (not 0). Remap if model returns 0.
            if result['rwp_score'] == 0:
                result['rwp_score'] = 1
                result['rwp_classification'] = 'UNWEIGHTED'
            
            # Update score distribution
            score_key = str(result['rwp_score'])
            if score_key in self.stats['score_distribution']:
                self.stats['score_distribution'][score_key] += 1
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"JSON Parse Error for {resume_path.name}: {e}")
            print(f"Response was: {response_text[:200]}")
            self.stats['api_errors'] += 1
            return None
        except Exception as e:
            print(f"Gemini API Error for {resume_path.name}: {e}")
            self.stats['api_errors'] += 1
            return None
    
    def normalize_name(self, name: str) -> str:
        """Normalize name for matching"""
        if not name:
            return ""
        # Remove special chars, convert to lowercase
        name = re.sub(r'[^\w\s]', '', name.lower())
        return ' '.join(name.split())
    
    def fuzzy_match_score(self, name1: str, name2: str) -> float:
        """Calculate fuzzy match score between two names"""
        n1 = self.normalize_name(name1)
        n2 = self.normalize_name(name2)
        return SequenceMatcher(None, n1, n2).ratio()
    
    def extract_name_from_filename(self, filename: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract name from resume filename as fallback"""
        # Remove extension and "Resume" prefix
        name = filename.replace('.pdf', '').replace('.PDF', '')
        name = re.sub(r'^Resume\s*', '', name, flags=re.IGNORECASE)
        name = name.strip()
        
        # Try to split into first/last
        # Handle patterns like "JohnDoe", "John_Doe", "John Doe"
        
        # CamelCase: "JohnDoe" -> "John Doe"
        name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
        
        # Underscores/dashes: "John_Doe" -> "John Doe"
        name = re.sub(r'[_-]', ' ', name)
        
        parts = name.split()
        if len(parts) >= 2:
            return parts[0], ' '.join(parts[1:])
        elif len(parts) == 1:
            return parts[0], None
        return None, None
    
    def find_candidate_match(self, first_name: str, last_name: str, client_id: str, 
                            resume_path: Path = None, resume_data: Dict = None,
                            confidence_threshold: float = 0.85, cross_client: bool = False):
        """
        Enhanced candidate matching with multiple strategies
        If cross_client=True, searches ALL clients (for legacy imports)
        Returns: (candidate_id, match_method, confidence, suggested_matches)
        """
        suggested_matches = []
        
        # Build WHERE clause based on cross_client flag
        client_filter = "" if cross_client else "AND client_id = :client_id"
        params_base = {"first_name": first_name, "last_name": last_name}
        if not cross_client:
            params_base["client_id"] = client_id
        
        with self.engine.connect() as conn:
            # Strategy 1: Exact match
            result = conn.execute(text(f"""
                SELECT id, first_name, last_name, email, client_id
                FROM candidates
                WHERE LOWER(first_name) = LOWER(:first_name)
                AND LOWER(last_name) = LOWER(:last_name)
                {client_filter}
            """), params_base)
            
            exact_match = result.fetchone()
            if exact_match:
                self.stats['matched_exact'] += 1
                if cross_client:
                    print(f"  🔗 Matched to client: {exact_match[4]}")
                return exact_match[0], 'exact', 1.0, []
            
            # Strategy 2: First name only match
            params_first = {"first_name": first_name}
            if not cross_client:
                params_first["client_id"] = client_id
            
            result = conn.execute(text(f"""
                SELECT id, first_name, last_name, email, client_id
                FROM candidates
                WHERE LOWER(first_name) = LOWER(:first_name)
                {client_filter}
            """), params_first)
            
            first_name_matches = result.fetchall()
            if len(first_name_matches) == 1:
                match = first_name_matches[0]
                self.stats['matched_first_name'] += 1
                if cross_client:
                    print(f"  🔗 Matched to client: {match[4]}")
                return match[0], 'first_name_only', 0.90, []
            elif len(first_name_matches) > 1:
                for m in first_name_matches:
                    suggested_matches.append({
                        'id': m[0],
                        'name': f"{m[1]} {m[2]}",
                        'email': m[3],
                        'client': m[4] if cross_client else None,
                        'reason': 'First name match'
                    })
            
            # Strategy 3: Fuzzy match on full name
            params_all = {} if cross_client else {"client_id": client_id}
            
            result = conn.execute(text(f"""
                SELECT id, first_name, last_name, email, client_id
                FROM candidates
                {"" if cross_client else "WHERE client_id = :client_id"}
            """), params_all)
            
            all_candidates = result.fetchall()
            
            best_match = None
            best_score = 0.0
            best_row = None
            
            for row in all_candidates:
                first_score = self.fuzzy_match_score(first_name, row[1])
                last_score = self.fuzzy_match_score(last_name, row[2]) if last_name else 0
                
                if last_name and len(last_name) > 1:
                    avg_score = (first_score + last_score) / 2
                else:
                    avg_score = first_score * 0.9
                
                if avg_score > best_score:
                    best_score = avg_score
                    best_match = row[0]
                    best_row = row
                
                if avg_score >= 0.6:
                    suggested_matches.append({
                        'id': row[0],
                        'name': f"{row[1]} {row[2]}",
                        'email': row[3],
                        'client': row[4] if cross_client else None,
                        'score': avg_score,
                        'reason': f'Fuzzy match ({avg_score:.0%})'
                    })
            
            if best_match and best_score >= confidence_threshold:
                self.stats['matched_fuzzy'] += 1
                if cross_client and best_row:
                    print(f"  🔗 Matched to client: {best_row[4]}")
                return best_match, 'fuzzy', best_score, []
            
            # Strategy 4: Try filename-based matching
            if resume_path:
                fn_first, fn_last = self.extract_name_from_filename(resume_path.name)
                if fn_first and fn_last and (fn_first.lower() != first_name.lower() or fn_last.lower() != (last_name or '').lower()):
                    result = conn.execute(text(f"""
                        SELECT id, first_name, last_name, email, client_id
                        FROM candidates
                        WHERE LOWER(first_name) = LOWER(:first_name)
                        AND LOWER(last_name) = LOWER(:last_name)
                        {client_filter}
                    """), {"first_name": fn_first, "last_name": fn_last} if cross_client else {"first_name": fn_first, "last_name": fn_last, "client_id": client_id})
                    
                    fn_match = result.fetchone()
                    if fn_match:
                        suggested_matches.insert(0, {
                            'id': fn_match[0],
                            'name': f"{fn_match[1]} {fn_match[2]}",
                            'email': fn_match[3],
                            'client': fn_match[4] if cross_client else None,
                            'reason': f'Filename match ({resume_path.name})'
                        })
            
            # No confident match found
            suggested_matches = sorted(
                suggested_matches, 
                key=lambda x: x.get('score', 0.5), 
                reverse=True
            )[:5]
            
            return None, 'none', 0.0, suggested_matches
    
    def update_candidate_with_resume_data(self, candidate_id: int, resume_data: Dict,
                                         resume_path: Path, match_method: str, confidence: float):
        """Update candidate record with parsed resume data in Supabase.
        Status promotion from 'No Resume':
          rwp_score >= 3  → Active
          rwp_score == 1  → Rejected (low_rwp)
          otherwise       → no status change
        Also sets tag = 'Driver' if tag is NULL."""
        rwp_score = resume_data.get('rwp_score')

        with self.engine.connect() as conn:
            # Fetch current status and tag so we only promote from No Resume
            row = conn.execute(text(
                "SELECT status, tag FROM candidates WHERE id = :id"
            ), {"id": candidate_id}).fetchone()

            current_status = row[0] if row else None
            current_tag = row[1] if row else None

            # Build dynamic SET clauses
            set_clauses = [
                "rwp_score = :rwp_score",
                "rwp_classification = :rwp_classification",
                "rwp_rationale = :rwp_rationale",
                "resume_filename = :resume_filename",
                "resume_processed_date = NOW()",
                "resume_match_method = :match_method",
                "resume_match_confidence = :confidence",
                "updated_at = NOW()",
            ]
            params = {
                "rwp_score": rwp_score,
                "rwp_classification": resume_data.get('rwp_classification'),
                "rwp_rationale": resume_data.get('rationale', ''),
                "resume_filename": resume_path.name,
                "match_method": match_method,
                "confidence": confidence,
                "id": candidate_id,
            }

            # Scorer does NOT change status -- only updates RWP fields
            # status = Active is set only at FADV submission
            # Reject low-score candidates regardless of current status
            if rwp_score == 1 and current_status == 'Intake':
                set_clauses.append("status = 'Rejected'")
                set_clauses.append("reject_reason = 'low_rwp'")
                print(f"  ⬇️  Rejected: low_rwp (rwp_score=1)")

            # Back-fill tag if missing
            if current_tag is None:
                set_clauses.append("tag = 'Driver'")

            sql = f"UPDATE candidates SET {', '.join(set_clauses)} WHERE id = :id"
            conn.execute(text(sql), params)
            conn.commit()
    
    def convert_to_pdf(self, file_path: Path, archive_folder: Path = None) -> Optional[Path]:
        """
        Convert a .docx, .doc, or .txt file to PDF using python-docx and reportlab.
        Archives the original to archive_folder if provided, deletes it otherwise.
        Returns the Path to the new PDF, or None on failure.
        """
        try:
            pdf_path = file_path.with_suffix('.pdf')

            # Extract text based on file type
            if file_path.suffix.lower() in ('.docx', '.doc'):
                try:
                    doc = DocxDocument(str(file_path))
                    paragraphs = [p.text for p in doc.paragraphs]
                except Exception as e:
                    print(f"  ⚠️  Could not read {file_path.name} as docx: {e}")
                    return None
            elif file_path.suffix.lower() == '.txt':
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    paragraphs = f.read().splitlines()
            else:
                return None

            # Build PDF with reportlab
            doc_pdf = SimpleDocTemplate(
                str(pdf_path),
                pagesize=letter,
                leftMargin=0.75 * inch,
                rightMargin=0.75 * inch,
                topMargin=0.75 * inch,
                bottomMargin=0.75 * inch
            )
            styles = getSampleStyleSheet()
            body_style = ParagraphStyle(
                'ResumeBody',
                parent=styles['Normal'],
                fontSize=10,
                leading=13,
                alignment=TA_LEFT
            )

            story = []
            for para_text in paragraphs:
                if para_text.strip():
                    # Escape XML special characters for reportlab
                    safe_text = (para_text
                                 .replace('&', '&amp;')
                                 .replace('<', '&lt;')
                                 .replace('>', '&gt;'))
                    story.append(Paragraph(safe_text, body_style))
                else:
                    story.append(Spacer(1, 6))

            if not story:
                story.append(Paragraph("(empty document)", body_style))

            doc_pdf.build(story)

            # Archive or delete original
            if archive_folder:
                archive_folder.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(archive_folder / file_path.name))
            else:
                file_path.unlink()

            print(f"  ✅ Converted {file_path.name} → {pdf_path.name}")
            return pdf_path

        except Exception as e:
            print(f"  ❌ Failed to convert {file_path.name} to PDF: {e}")
            return None

    def process_resumes(self, resume_folder: Path, client_id: str, limit: int = None, archive_folder: Path = None):
        """
        Process all resumes in folder for given client
        If client_id is 'legacy', searches ALL clients for matches
        
        Args:
            resume_folder: Path to folder containing resumes
            client_id: Client identifier (use 'legacy' for cross-client matching)
            limit: Max number of resumes to process (None = no limit)
            archive_folder: Where to move processed resumes (None = don't move)
        """
        # --- Pre-pass: convert non-PDF resumes to PDF ---
        non_pdf_exts = ('.docx', '.doc', '.txt')
        non_pdf_files = [
            f for f in resume_folder.iterdir()
            if f.is_file() and f.suffix.lower() in non_pdf_exts
        ] if resume_folder.exists() else []

        converted_count = 0
        for nf in non_pdf_files:
            result = self.convert_to_pdf(nf, archive_folder=archive_folder)
            if result:
                converted_count += 1

        if non_pdf_files:
            print(f"\n📄 Pre-pass: converted {converted_count}/{len(non_pdf_files)} non-PDF files to PDF")

        if not resume_folder.exists():
            print(f"Resume folder not found: {resume_folder}")
            return

        # Check if this is legacy mode (cross-client matching)
        cross_client = client_id.lower() == 'legacy'
        if cross_client:
            print("\n🔄 LEGACY MODE: Cross-client matching enabled")
        
        # Find all PDF files and sort alphabetically
        resume_files = list(resume_folder.glob('*.pdf')) + list(resume_folder.glob('*.PDF'))
        resume_files = sorted(resume_files, key=lambda x: x.name.lower())
        self.stats['resumes_found'] = len(resume_files)
        
        if not resume_files:
            print(f"No resumes found in {resume_folder}")
            return
        
        print(f"\nProcessing {len(resume_files)} resumes for client: {client_id}")
        print("=" * 60)
        
        for idx, resume_path in enumerate(resume_files, 1):
            # Check limit
            if limit and idx > limit:
                print(f"\n⏸️  Limit reached ({limit} resumes). Stopping.")
                break
                
            print(f"\n[{idx}/{len(resume_files)}] Processing: {resume_path.name}")
            
            # Parse resume with Gemini
            resume_data = self.parse_resume_with_gemini(resume_path)
            
            if not resume_data:
                print(f"  ❌ Failed to parse resume")
                # Move failed resume to errors folder
                if archive_folder:
                    error_folder = archive_folder.parent.parent / "99_ERRORS" / client_id
                    error_folder.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.move(str(resume_path), str(error_folder / resume_path.name))
                        print(f"  📁 Moved to errors folder")
                    except Exception as e:
                        print(f"  ⚠️  Could not move file: {e}")
                continue
            
            print(f"  📄 Parsed: {resume_data['first_name']} {resume_data['last_name']}")
            print(f"  📊 RWP: {resume_data['rwp_score']} ({resume_data['rwp_classification']})")
            
            # Find matching candidate with enhanced matching
            candidate_id, match_method, confidence, suggestions = self.find_candidate_match(
                resume_data['first_name'],
                resume_data['last_name'],
                client_id,
                resume_path=resume_path,
                resume_data=resume_data,
                cross_client=cross_client
            )
            
            if candidate_id:
                print(f"  ✅ Match: {match_method} (confidence: {confidence:.2f})")
                self.update_candidate_with_resume_data(
                    candidate_id,
                    resume_data,
                    resume_path,
                    match_method,
                    confidence
                )
            else:
                print(f"  ⚠️  No match found in database")
                
                # If create_unmatched flag is set, create new candidate record
                if self.create_unmatched:
                    created = self.create_candidate_from_resume(resume_data, client_id, resume_path)
                    if created:
                        # Move to archive since we created the record
                        if archive_folder:
                            try:
                                shutil.move(str(resume_path), str(archive_folder / resume_path.name))
                                print(f"  📁 Archived")
                            except Exception as e:
                                print(f"  ⚠️  Could not archive: {e}")
                        continue  # Skip adding to unmatched list
                
                # Store for review report (only if not created)
                self.unmatched_resumes.append({
                    'filename': resume_path.name,
                    'parsed_name': f"{resume_data['first_name']} {resume_data['last_name']}",
                    'rwp_score': resume_data['rwp_score'],
                    'rwp_classification': resume_data['rwp_classification'],
                    'rationale': resume_data.get('rationale', ''),
                    'phone': resume_data.get('phone', ''),
                    'email': resume_data.get('email', ''),
                    'suggestions': suggestions,
                    'client_id': client_id
                })
                
                self.stats['unmatched'] += 1
                
                if suggestions:
                    print(f"  💡 Suggested matches:")
                    for sug in suggestions[:3]:
                        print(f"      - {sug['name']} ({sug['reason']})")
            
            # Move processed resume to archive immediately
            if archive_folder:
                try:
                    shutil.move(str(resume_path), str(archive_folder / resume_path.name))
                    print(f"  📁 Archived")
                except Exception as e:
                    print(f"  ⚠️  Could not archive: {e}")
        
        # Print summary
        print("\n" + "=" * 60)
        print("RESUME PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Resumes found: {self.stats['resumes_found']}")
        print(f"Matched (exact): {self.stats['matched_exact']}")
        print(f"Matched (fuzzy): {self.stats['matched_fuzzy']}")
        print(f"Matched (first name): {self.stats['matched_first_name']}")
        print(f"Created (new): {self.stats['created']}")
        print(f"Unmatched: {self.stats['unmatched']}")
        print(f"API calls: {self.stats['api_calls']}")
        print(f"API errors: {self.stats['api_errors']}")
        print(f"\nScore Distribution (RWP v2.1):")
        for score, count in sorted(self.stats['score_distribution'].items(), key=lambda x: int(x[0]), reverse=True):
            if count > 0:
                label = {
                    '11': 'FEDEX_ACTIVE',
                    '10': 'FEDEX_FORMER',
                    '9': 'DELIVERY_EXP',
                    '7': 'WAREHOUSE_EXP',
                    '6': 'COMMERCIAL_DRIVER',
                    '3': 'LOW_RELEVANCE',
                    '1': 'UNWEIGHTED'
                }.get(score, 'UNKNOWN')
                print(f"  {score} ({label}): {count}")
        print("=" * 60)
        
        # Generate unmatched review report if needed
        if self.unmatched_resumes:
            self.generate_unmatched_report(client_id)
    
    def generate_unmatched_report(self, client_id: str):
        """Generate a review report for unmatched resumes"""
        # Use home directory for logs since we're not using local db anymore
        report_dir = Path.home() / "Library" / "CloudStorage" / "GoogleDrive-charles@thefoundry.llc" / "My Drive" / "PEAK" / "#PEAKATS" / "00_SYSTEM" / "logs"
        report_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        report_path = report_dir / f"UNMATCHED_REVIEW_{client_id}_{timestamp}.txt"
        
        print(f"\n{'='*60}")
        print("⚠️  UNMATCHED RESUMES - MANUAL REVIEW REQUIRED")
        print(f"{'='*60}")
        
        report_lines = [
            "=" * 70,
            f"UNMATCHED RESUME REVIEW REPORT",
            f"Client: {client_id}",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Unmatched: {len(self.unmatched_resumes)}",
            "=" * 70,
            "",
            "INSTRUCTIONS:",
            "Review each unmatched resume below. For each one, either:",
            "  1. Run the suggested SQL fix if a match is found",
            "  2. Manually add the candidate if they're new",
            "  3. Ignore if the resume is invalid/duplicate",
            "",
            "-" * 70,
        ]
        
        for idx, unmatched in enumerate(self.unmatched_resumes, 1):
            print(f"\n[{idx}] {unmatched['filename']}")
            print(f"    Parsed as: {unmatched['parsed_name']}")
            print(f"    RWP Score: {unmatched['rwp_score']} ({unmatched['rwp_classification']})")
            
            report_lines.extend([
                "",
                f"[{idx}] FILE: {unmatched['filename']}",
                f"    Parsed Name: {unmatched['parsed_name']}",
                f"    RWP Score: {unmatched['rwp_score']} ({unmatched['rwp_classification']})",
                f"    Rationale: {unmatched['rationale']}",
                f"    Phone: {unmatched['phone']}",
                f"    Email: {unmatched['email']}",
                "",
            ])
            
            if unmatched['suggestions']:
                print(f"    💡 SUGGESTED MATCHES:")
                report_lines.append("    SUGGESTED MATCHES:")
                
                for sug in unmatched['suggestions']:
                    print(f"       → {sug['name']} - {sug['reason']}")
                    report_lines.append(f"       → {sug['name']} ({sug['email']}) - {sug['reason']}")
                    
                    # Generate SQL fix command
                    sql_fix = f"""
    -- FIX: Link resume to {sug['name']}
    UPDATE candidates 
    SET rwp_score = {unmatched['rwp_score']},
        rwp_classification = '{unmatched['rwp_classification']}',
        resume_filename = '{unmatched['filename']}',
        resume_processed_date = datetime('now'),
        resume_match_method = 'manual_review',
        updated_at = datetime('now')
    WHERE id = {sug['id']};
"""
                    report_lines.append(sql_fix)
            else:
                print(f"    ❌ No suggested matches found")
                report_lines.append("    ❌ No suggested matches - may be a new candidate")
            
            report_lines.append("-" * 70)
        
        # Write report to file
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        print(f"\n📋 Review report saved: {report_path}")
        print(f"   Open this file to see suggested SQL fixes")


def process_resumes(resume_folder: Path, client_id: str, db_path: Path = None, limit: int = None, archive_folder: Path = None, create_unmatched: bool = False):
    """
    Main entry point for resume processing
    Called by peak_process_batch.py
    Now writes directly to Supabase (db_path ignored)
    
    Args:
        resume_folder: Path to folder containing resumes
        client_id: Client identifier
        db_path: Ignored (kept for compatibility)
        limit: Max resumes to process (None = no limit)
        archive_folder: Where to move processed resumes
        create_unmatched: If True, create new candidate records for unmatched resumes
    """
    processor = ResumeProcessor(create_unmatched=create_unmatched)
    processor.connect_db()
    
    try:
        processor.process_resumes(resume_folder, client_id, limit=limit, archive_folder=archive_folder)
    finally:
        processor.close_db()
    
    return processor.stats


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("""
PEAKATS Resume Intelligence Gateway (RIG) v3.1
RWP Scoring v2.1 (Updated Feb 28, 2026)

Usage: python peak_rig_processor_v2.py <resume_folder> <client_id> [--limit N] [--create-unmatched]

RWP Scale:
  11 = FEDEX_ACTIVE (Current FedEx Driver)
  10 = FEDEX_FORMER (Past FedEx Driver / FedEx Handler)
   9 = DELIVERY_EXP (UPS, Amazon, USPS drivers)
   7 = WAREHOUSE_EXP (Warehouse, dock, sorter)
   6 = COMMERCIAL_DRIVER (CDL, box truck)
   3 = LOW_RELEVANCE (Gig only)
   1 = UNWEIGHTED (No experience — reviewed)
   0 = UNSCORED (Never reviewed)
        """)
        sys.exit(1)
    
    resume_folder = Path(sys.argv[1])
    client_id = sys.argv[2]
    
    # Parse optional limit
    limit = None
    if '--limit' in sys.argv:
        limit_idx = sys.argv.index('--limit')
        if limit_idx + 1 < len(sys.argv):
            limit = int(sys.argv[limit_idx + 1])
    
    # Parse create-unmatched flag
    create_unmatched = '--create-unmatched' in sys.argv
    
    stats = process_resumes(resume_folder, client_id, limit=limit, create_unmatched=create_unmatched)
    print(f"\n✅ Processing complete!")
