# utils/schema_and_prompt.py
DESCRIPTORS = {
    "Case_description"   : "Patient/case description without figures/tables, no genetics, no disease name.",
    "Genetic_validation" : "Does the paper report genetic validation? (yes/no).",
    "Responsible_gene"   : "Gene symbol as written in the paper.",
    "Underlying_disease" : "Disease name as written in the paper.",
    "OMIM"               : "Leave blank; will be resolved via the internet.",
    "OrphaNet"           : "Leave blank; will be resolved via the internet.",
    "Reference_title"    : "Paper title.",
    "PubMed_ID"          : "Leave blank; will be resolved via PubMed.",
    "Single-patient case report": "Is it a single-patient report? (yes/no).",
    "Source_file": "Original PDF filename (set by pipeline, not the model).",
}
COLUMNS = list(DESCRIPTORS.keys())

PROMPT = (
    "Extract the following fields as JSON from the article text below.\n"
    "Rules:\n"
    "• Case_description must omit figures/tables and any genetics or disease name.\n"
    "• Genetic_validation and Single-patient case report must be EXACTLY one of: \"yes\" or \"no\" (lowercase). "
    "If unknown, use an empty string.\n"
    "• OMIM, OrphaNet, PubMed_ID must be empty strings.\n"
    "• Return ONLY valid JSON with these keys (all required) and string values: "
    f"{', '.join(COLUMNS)}\n"
    "Do not include any explanations.\n\n"
    "Article text:\n"
)