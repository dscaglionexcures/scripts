"""
Generate a Medication Dispense Summary CCD-A:

1. Build C-CDA XML (simplified example).
2. Write XML to disk.
3. Render a human readable PDF similar to the reference layout.
4. Embed the XML file as an attachment inside the PDF.

"""

from datetime import datetime, timezone
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

from pypdf import PdfReader, PdfWriter


# ---------------------------------------------------------------------------
# 1. Data model
# ---------------------------------------------------------------------------

data = {
    "document_title": "Medication Dispense Summary (CCD-A)",
    "document_code": "34133-9",
    "document_code_display": "Summarization of episode note",

    "patient_name": "David Paul Scaglione",
    "patient_dob": "1977-03-24",
    "patient_gender": "Male",
    "patient_mrn": "MRN0985731",

    "prescriber_role_line1": "Prescribing",
    "prescriber_role_line2": "Physician",
    "prescriber_name": "Dr Pulak D Patel",
    "prescriber_npi": "1023377918",
    "prescriber_org": "Novant Health System - South Blvd Office",
    "prescriber_addr_line1": "2400 South Blvd., Suite 103",
    "prescriber_addr_line2": "Charlotte, NC 28203",
    "prescriber_phone": "704-316-3017",
    "prescriber_fax": "704-316-3018",

    "pharmacist_role_line1": "Dispensing",
    "pharmacist_role_line2": "Pharmacist",
    "pharmacist_name": "Sarah Thomas, RPh",
    "pharmacist_npi": "1928374650",
    "pharmacy_name": "EVERSANA Specialty Pharmacy",
    "pharmacy_addr_line1": "17877 CHESTERFIELD AIRPORT RD STE A",
    "pharmacy_addr_line2": "CHESTERFIELD, MO 63005-1211",
    "pharmacy_phone": "513-285-1889",
    "pharmacy_fax": "877-473-3172",

    "med_name": "Ayvakit (Avapritinib) 25 mg tablet",
    "med_rxcui": "2559725",
    "med_ndc": "72064-0125-30",
    "med_qty": "30 tablets",
    "med_dispense_date": "2025-10-31",
    "med_rx_number": "80578291029",
    "med_loinc_event": "29304-3",
    "med_loinc_event_display": "Medication dispensed",

    "med_sig_display": (
        "Sig: Take one (1) tablet by mouth once daily on an empty stomach, "
        "at least 1 hour before or 2 hours after a meal. Swallow\n"
        "tablet whole; do not crush, chew, or split."
    ),
    "med_sig_xml": (
        "Take one (1) tablet by mouth once daily on an empty stomach, "
        "at least 1 hour before or 2 hours after a meal. Swallow tablet whole; "
        "do not crush, chew, or split."
    ),
    "refills_authorized": "12",
    "refills_remaining": "11",

    "encounter_location": (
        "EVERSANA Specialty Pharmacy - 17877 CHESTERFIELD AIRPORT RD STE A, "
        "CHESTERFIELD, MO 63005-1211"
    ),
    "practice_site": (
        "Novant Health System - South Blvd Office - 2400 South Blvd., Suite 103, "
        "Charlotte, NC 28203"
    ),

    "xml_filename": "medication_dispense_ccda.xml",
    "pdf_filename": "medication_dispense_summary_ccda.pdf",
}


# ---------------------------------------------------------------------------
# 2. XML builder
# ---------------------------------------------------------------------------

def build_ccda_xml(d: dict) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="urn:hl7-org:v3 CDA.xsd">
  <typeId root="2.16.840.1.113883.1.3" extension="POCD_HD000040"/>
  <templateId root="2.16.840.1.113883.10.20.22.1.2"/>

  <id root="2.16.840.1.113883.19.5" extension="MedicationDispenseSummary"/>
  <code code="{d['document_code']}"
        codeSystem="2.16.840.1.113883.6.1"
        displayName="{d['document_code_display']}"/>

  <title>{d['document_title']}</title>
  <effectiveTime value="{ts}"/>
  <confidentialityCode code="N" codeSystem="2.16.840.1.113883.5.25"/>
  <languageCode code="en-US"/>

  <recordTarget>
    <patientRole>
      <id root="2.16.840.1.113883.19.5" extension="{d['patient_mrn']}"/>
      <patient>
        <name>
          <given>{d['patient_name'].split()[0]}</given>
          <family>{" ".join(d['patient_name'].split()[1:])}</family>
        </name>
        <administrativeGenderCode code="{d['patient_gender'][0].upper()}"/>
        <birthTime value="{d['patient_dob'].replace('-', '')}"/>
      </patient>
    </patientRole>
  </recordTarget>

  <author>
    <time value="{ts}"/>
    <assignedAuthor>
      <id root="2.16.840.1.113883.4.6" extension="{d['prescriber_npi']}"/>
      <assignedPerson>
        <name>
          <prefix>Dr</prefix>
          <given>{d['prescriber_name'].split()[1]}</given>
          <family>{d['prescriber_name'].split()[-1]}</family>
        </name>
      </assignedPerson>
      <representedOrganization>
        <name>{d['prescriber_org']}</name>
      </representedOrganization>
    </assignedAuthor>
  </author>

  <custodian>
    <assignedCustodian>
      <representedCustodianOrganization>
        <id root="2.16.840.1.113883.19.5" extension="{d['pharmacy_name']}"/>
        <name>{d['pharmacy_name']}</name>
      </representedCustodianOrganization>
    </assignedCustodian>
  </custodian>

  <component>
    <structuredBody>
      <component>
        <section>
          <templateId root="2.16.840.1.113883.10.20.22.2.37"/>
          <code code="{d['med_loinc_event']}"
                codeSystem="2.16.840.1.113883.6.1"
                displayName="{d['med_loinc_event_display']}"/>
          <title>Medication Dispensed</title>
          <text>
            <paragraph>Medication: {d['med_name']}</paragraph>
            <paragraph>RxCUI: {d['med_rxcui']}</paragraph>
            <paragraph>NDC: {d['med_ndc']}</paragraph>
            <paragraph>Quantity Dispensed: {d['med_qty']}</paragraph>
            <paragraph>Date Dispensed: {d['med_dispense_date']}</paragraph>
            <paragraph>Prescription Number: {d['med_rx_number']}</paragraph>
            <paragraph>Sig: {d['med_sig_xml']}</paragraph>
            <paragraph>Refills Authorized: {d['refills_authorized']}</paragraph>
            <paragraph>Refills Remaining: {d['refills_remaining']}</paragraph>
          </text>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>
"""
    return xml.strip() + "\n"


# ---------------------------------------------------------------------------
# 3. Drawing helpers
# ---------------------------------------------------------------------------

def draw_multiline(c, text, x, y, width,
                   leading=12, font_name="Helvetica", font_size=10):
    from textwrap import wrap

    c.setFont(font_name, font_size)
    chars = max(1, int(width / (font_size * 0.5)))

    lines = []
    for para in text.split("\n"):
        para = para.strip()
        if not para:
            lines.append("")
        else:
            lines.extend(wrap(para, chars))

    cy = y
    for line in lines:
        c.drawString(x, cy, line)
        cy -= leading

    return cy


def draw_care_team_row(c, y, d, is_prescriber=True):
    leading = 11

    x_role = 0.75 * inch
    x_name = x_role + 1.7 * inch
    x_org = x_role + 3.6 * inch
    x_phone = x_role + 6.1 * inch

    w_role = 1.6 * inch
    w_name = 1.7 * inch
    w_org = 2.3 * inch
    w_phone = 1.0 * inch

    if is_prescriber:
        role_t = f"{d['prescriber_role_line1']}\n{d['prescriber_role_line2']}"
        name_t = f"{d['prescriber_name']}\nNPI {d['prescriber_npi']}"
        org_t = (
            f"{d['prescriber_org']}\n"
            f"{d['prescriber_addr_line1']}\n"
            f"{d['prescriber_addr_line2']}"
        )
        phone_t = f"{d['prescriber_phone']}\n{d['prescriber_fax']}"
    else:
        role_t = f"{d['pharmacist_role_line1']}\n{d['pharmacist_role_line2']}"
        name_t = f"{d['pharmacist_name']}\nNPI {d['pharmacist_npi']}"
        org_t = (
            f"{d['pharmacy_name']}\n"
            f"{d['pharmacy_addr_line1']}\n"
            f"{d['pharmacy_addr_line2']}"
        )
        phone_t = f"{d['pharmacy_phone']}\n{d['pharmacy_fax']}"

    c.setFont("Helvetica", 10)

    role_y = draw_multiline(c, role_t, x_role, y, w_role, leading=leading)
    name_y = draw_multiline(c, name_t, x_name, y, w_name, leading=leading)
    org_y = draw_multiline(c, org_t, x_org, y, w_org, leading=leading)
    phone_y = draw_multiline(c, phone_t, x_phone, y, w_phone, leading=leading)

    return min(role_y, name_y, org_y, phone_y) - 0.15 * inch


# ---------------------------------------------------------------------------
# 4. PDF generator
# ---------------------------------------------------------------------------

def create_pdf(d, xml_content):
    pdf_path = Path(d["pdf_filename"])
    xml_path = Path(d["xml_filename"])

    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    width, height = letter

    margin_left = 0.75 * inch
    margin_right = 0.75 * inch
    max_text_width = width - margin_left - margin_right

    # Box geometry
    box_x = margin_left - 0.15 * inch
    box_width = width - margin_left - margin_right + 0.3 * inch
    padding_top = 0.1 * inch
    padding_bottom = 0.15 * inch

    c.setLineWidth(0.5)

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------
    y = height - 0.9 * inch
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin_left, y, d["document_title"])

    y -= 0.35 * inch
    c.setFont("Helvetica", 10)
    code_line = f"Document Code: LOINC {d['document_code']} - {d['document_code_display']}"
    c.drawString(margin_left, y, code_line)

    y -= 0.22 * inch
    c.drawString(
        margin_left,
        y,
        "Machine-readable C-CDA XML is embedded (Attachments).",
    )

    # ------------------------------------------------------------------
    # Patient section (boxed, layout unchanged)
    # ------------------------------------------------------------------
    y -= 0.45 * inch
    patient_box_top = y + padding_top

    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin_left, y, "Patient")

    c.setFont("Helvetica", 10)
    label_indent = margin_left
    value_indent = margin_left

    # Name
    y -= 0.25 * inch
    c.drawString(label_indent, y, "Name")
    y -= 0.18 * inch
    c.drawString(value_indent, y, d["patient_name"])

    # DOB
    y -= 0.24 * inch
    c.drawString(label_indent, y, "DOB")
    y -= 0.18 * inch
    c.drawString(value_indent, y, d["patient_dob"])

    # Gender
    y -= 0.24 * inch
    c.drawString(label_indent, y, "Gender")
    y -= 0.18 * inch
    c.drawString(value_indent, y, d["patient_gender"])

    # MRN
    y -= 0.24 * inch
    c.drawString(label_indent, y, "MRN")
    y -= 0.18 * inch
    c.drawString(value_indent, y, d["patient_mrn"])

    patient_box_bottom = y - padding_bottom
    c.rect(
        box_x,
        patient_box_bottom,
        box_width,
        patient_box_top - patient_box_bottom,
        stroke=1,
        fill=0,
    )

    # ------------------------------------------------------------------
    # Care Team (boxed, layout unchanged)
    # ------------------------------------------------------------------
    y -= 0.5 * inch
    care_box_top = y + padding_top

    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin_left, y, "Care Team")

    y -= 0.3 * inch
    c.setFont("Helvetica-Bold", 10)

    x_role = margin_left
    x_name = margin_left + 1.7 * inch
    x_org = margin_left + 3.6 * inch
    x_phone = margin_left + 6.1 * inch

    c.drawString(x_role, y, "Role")
    c.drawString(x_name, y, "Name / NPI")
    c.drawString(x_org, y, "Organization / Location")
    c.drawString(x_phone, y, "Phone / Fax")

    # Prescriber row
    y -= 0.25 * inch
    y = draw_care_team_row(c, y, d, is_prescriber=True)

    # Pharmacist row
    y = draw_care_team_row(c, y, d, is_prescriber=False)

    care_box_bottom = y - padding_bottom
    c.rect(
        box_x,
        care_box_bottom,
        box_width,
        care_box_top - care_box_bottom,
        stroke=1,
        fill=0,
    )

    # ------------------------------------------------------------------
    # Medication Dispensed (boxed, with med name)
    # ------------------------------------------------------------------
    y -= 0.6 * inch
    med_box_top = y + padding_top

    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin_left, y, "Medication Dispensed")

    y -= 0.28 * inch
    c.setFont("Helvetica", 10)
    code_display_line = f"{d['med_loinc_event']} - {d['med_loinc_event_display']}"
    c.drawString(margin_left, y, code_display_line)

    # Explicit medication name line
    y -= 0.22 * inch
    c.drawString(margin_left, y, f"Medication: {d['med_name']}")

    med_box_bottom = y - padding_bottom
    c.rect(
        box_x,
        med_box_bottom,
        box_width,
        med_box_top - med_box_bottom,
        stroke=1,
        fill=0,
    )

    # ------------------------------------------------------------------
    # Sig and Refills (boxed, layout unchanged except box)
    # ------------------------------------------------------------------
    y -= 0.55 * inch
    sig_box_top = y + padding_top

    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin_left, y, "Sig and Refills")

    y -= 0.28 * inch
    c.setFont("Helvetica", 10)

    # Sig multiline
    y = draw_multiline(
        c,
        d["med_sig_display"],
        margin_left,
        y,
        width=max_text_width,
        leading=12,
        font_size=10,
    )

    # Refills
    y -= 0.24 * inch
    c.drawString(margin_left, y, "Refills Authorized")
    y -= 0.18 * inch
    c.drawString(margin_left, y, d["refills_authorized"])

    y -= 0.26 * inch
    c.drawString(margin_left, y, "Refills Remaining (post-dispense)")
    y -= 0.18 * inch
    c.drawString(margin_left, y, d["refills_remaining"])

    sig_box_bottom = y - padding_bottom
    c.rect(
        box_x,
        sig_box_bottom,
        box_width,
        sig_box_top - sig_box_bottom,
        stroke=1,
        fill=0,
    )

    # ------------------------------------------------------------------
    # Encounter Location (boxed, layout unchanged)
    # ------------------------------------------------------------------
    y -= 0.55 * inch
    enc_box_top = y + padding_top

    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin_left, y, "Encounter Location")

    c.setFont("Helvetica", 10)

    # Encounter Location
    y -= 0.28 * inch
    c.drawString(margin_left, y, "Encounter Location")
    y -= 0.18 * inch
    y = draw_multiline(
        c,
        d["encounter_location"],
        margin_left,
        y,
        width=max_text_width,
        leading=12,
        font_size=10,
    )

    # Practice Site
    y -= 0.28 * inch
    c.drawString(margin_left, y, "Practice Site")
    y -= 0.18 * inch
    y = draw_multiline(
        c,
        d["practice_site"],
        margin_left,
        y,
        width=max_text_width,
        leading=12,
        font_size=10,
    )

    enc_box_bottom = y - padding_bottom
    c.rect(
        box_x,
        enc_box_bottom,
        box_width,
        enc_box_top - enc_box_bottom,
        stroke=1,
        fill=0,
    )

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------
    generated_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    footer = f"Generated: {generated_str}"
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin_right, 0.75 * inch, footer)

    c.showPage()
    c.save()

    # ------------------------------------------------------------------
    # Attach XML with pypdf
    # ------------------------------------------------------------------
    xml_path.write_text(xml_content, encoding="utf-8")

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)

    with xml_path.open("rb") as f:
        writer.add_attachment(d["xml_filename"], f.read())

    with pdf_path.open("wb") as f:
        writer.write(f)


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def main():
    xml_content = build_ccda_xml(data)
    create_pdf(data, xml_content)
    print(f"Wrote XML to: {data['xml_filename']}")
    print(f"Wrote PDF with embedded XML to: {data['pdf_filename']}")


if __name__ == "__main__":
    main()
