# Innovative & Emerging Clinical Trial Designs

## Overview

The landscape of clinical trial methodology has undergone a profound transformation over the past two decades, driven by escalating drug development costs, complex research questions, the limitations of traditional randomized controlled trials (RCTs), and advances in digital technology, biostatistics, and data infrastructure. Innovative trial designs aim to be more flexible, faster, more ethical (minimizing patient exposure to inferior treatments), and more efficient at answering multifaceted scientific questions. Regulatory bodies — particularly the FDA and EMA — have actively encouraged their adoption through dedicated guidance documents and pilot programs.[^1][^2][^3][^4][^5][^6]

This report catalogs established innovative designs, along with the latest and most emerging approaches shaping clinical development through 2025–2026.

***

## Part I: Adaptive Designs

Adaptive designs are the broadest and most widely adopted category of innovative trial methodology. The FDA defines an adaptive design as one that "allows for prospectively planned modifications to one or more aspects of the design based on accumulating data from subjects in the trial". These pre-specified adaptations may affect dosage, sample size, randomization ratios, patient selection, endpoints, and even the number of arms — all governed by statistical rules established before trial initiation.[^2][^7][^4]

### Sample Size Re-estimation

One of the most foundational adaptive elements, sample size re-estimation allows a trial to adjust its enrollment target at one or more pre-planned interim analyses based on the observed effect size or variance. This prevents underpowering (if early effects are smaller than anticipated) or over-enrollment (if effects are larger). It is commonly used in confirmatory Phase III trials and is broadly accepted by the FDA and EMA.[^4][^8]

### Adaptive Randomization / Response-Adaptive Randomization (RAR)

Response-Adaptive Randomization (RAR) dynamically adjusts treatment allocation probabilities in favor of arms showing better interim outcomes. This is ethically appealing — more patients are assigned to better-performing arms — and scientifically efficient for multi-arm trials. RAR is predominantly used in Phase II oncology and rare disease studies, with approximately 85% of published RAR trials employing Bayesian statistical methods. Despite its scientific appeal, adoption lags behind other adaptive designs due to concerns about time-trend bias and type I error inflation, which has spurred recent methodological guidance.[^9][^10][^11]

### Adaptive Dose-Finding (Including Model-Informed Designs)

Traditional "3+3" dose escalation designs have long dominated oncology Phase I trials but provide limited insight into dose-response relationships. Emerging designs, including Bayesian Optimal Interval (BOIN), Continual Reassessment Method (CRM), and escalation with overdose control (EWOC), leverage real-time accumulating data to optimize dose selection. The FDA's **Project Optimus** initiative (launched 2021) formally shifted the paradigm in oncology away from Maximum Tolerated Dose (MTD) toward an Optimal Biological Dose (OBD) framework, requiring sponsors to conduct randomized, data-driven dose-ranging evaluations before pivotal Phase III trials. Multi-Arm Two-Stage (MATS) designs have been specifically proposed to meet Project Optimus requirements, allowing simultaneous evaluation of two selected doses across multiple indications in a Bayesian hierarchical framework.[^12][^13][^14][^15][^16]

### Drop-the-Losers / Add-the-Winners

These designs begin with multiple arms and use interim data to prune inferior arms (drop-the-losers) or add new, more promising arms (add-the-winners). They maximize efficiency in mid-development when optimal doses, treatment combinations, or patient segments are uncertain.[^8][^4]

### Population Enrichment Designs

Enrichment designs prospectively narrow the enrolled population, mid-trial, to subgroups most likely to benefit — typically defined by a biomarker. In the era of precision medicine, this is increasingly critical: if a targeted therapy is effective only in a biomarker-positive subgroup, an enrichment design concentrates resources on the most informative patients while maintaining the option to evaluate the broader population. The FDA's guidance distinguishes several enrichment subtypes: prognostic (selecting patients most likely to have outcomes), predictive (selecting patients most likely to respond), and multiple-biomarker adaptive thresholding designs.[^17][^18][^19][^4]

### Seamless Phase 2/3 Adaptive Designs

Traditional drug development separates Phase 2 (learning/dose-finding) and Phase 3 (confirmatory) into distinct, sequential trials with substantial gaps between them. **Seamless adaptive designs** collapse these two stages into a single trial, enabling Phase 2 data to inform Phase 3 design adaptations — and, in inferentially seamless designs, contribute directly to the final Phase 3 analysis. This can save significant time and cost, as Phase 2 patients enrolled before the adaptation remain in the combined analysis. Operational challenges remain significant, including the need for robust interim analysis firewalls and pre-specification of decision rules.[^20][^21][^22][^23]

***

## Part II: Master Protocol Designs

Master protocols are overarching frameworks that allow multiple sub-studies, populations, or interventions to be evaluated simultaneously under a single regulatory and operational structure. The FDA released dedicated guidance on master protocol designs in 2020, recognizing them as a major category of innovative design. Three primary architectures exist:[^24][^25]

### Basket Trials

Basket trials enroll patients based on a shared molecular or genetic feature (the "basket"), regardless of tumor type or organ site. A single investigation drug is tested across multiple disease cohorts simultaneously, enabling rapid signal-finding for biomarker-driven therapies. This design is especially prominent in oncology and rare disease development. Key examples include the NCI-MATCH trial (Molecular Analysis for Therapy Choice), which assigns patients across dozens of tumor types based on gene mutations.[^24]

### Umbrella Trials

Umbrella trials operate on the reverse logic: they enroll patients with a single disease but stratify them into multiple sub-studies based on biomarker profiling, with each sub-study testing a different targeted therapy matched to the biomarker. This allows a single disease (e.g., non-small cell lung cancer) to serve as the umbrella under which multiple precision therapies compete. The BATTLE trial in lung cancer is an early archetype.[^26][^24]

### Platform Trials

Platform trials are the most flexible and durable master protocol design. They combine features of basket and umbrella trials and operate on a **perpetual or standing** basis — treatments can be added to or removed from the platform as evidence accumulates, without requiring a completely new trial. The trial framework (protocol, regulatory submission, data infrastructure, statistical model) persists indefinitely:[^27][^26][^24]

- **REMAP-CAP** (Randomized Embedded Multifactorial Adaptive Platform trial for Community-Acquired Pneumonia): A landmark global platform trial spanning 300 sites in 19 countries. It uses Bayesian response-adaptive randomization and evaluates multiple treatment "domains" (combinations of interventions) simultaneously. During COVID-19, its pre-specified Pandemic Appendix was activated, enabling rapid identification of effective treatments — including tocilizumab — with relatively small patient numbers.[^28][^29]
- **RECOVERY**: The UK-based platform trial identified multiple effective (and ineffective) treatments for COVID-19, including dexamethasone, in record time through a highly streamlined, pragmatic design.[^30][^28]
- **GBM AGILE**: An international, seamless Phase II/III response-adaptive randomization platform trial for glioblastoma, using Bayesian adaptive randomization within disease subtypes to test multiple therapies under a single master IND.[^31][^32]
- **AGILE**: An international multi-arm, multi-dose, multi-stage, adaptive Bayesian randomized platform trial for antiviral candidates, where dose finding and efficacy evaluation are conducted seamlessly.[^33]

***

## Part III: Bayesian Trial Designs

While many adaptive designs use Bayesian methods, **Bayesian trial designs** as a distinct category refer to trials in which Bayesian inference structures the core decision-making: prior probability distributions are combined with accumulating trial data to update posterior probabilities and inform stopping, adaptation, or escalation rules. This contrasts with the frequentist paradigm of traditional RCTs.[^34][^35]

Key features include:
- **Continuous updating**: Posterior probabilities are recalculated as data accumulate, enabling more efficient futility stopping than frequentist designs — especially valuable in rare diseases where committing patients to long-running "negative" trials denies them access to more promising studies.[^36]
- **Bayesian hierarchical models**: Allow information borrowing across arms, indications, or populations — as in the MATS design for oncology dose optimization and across pediatric/adult populations in rare disease programs.[^12]
- **Prior elicitation**: Formally incorporates historical evidence (from earlier trials, animal studies, or real-world data) as a starting point for inference.[^37]

The FDA's guidance on adaptive designs explicitly addresses Bayesian adaptive and complex trials, including requirements for computer simulations to validate operating characteristics before trial launch.[^2]

***

## Part IV: Decentralized, Hybrid, and Virtual Trial Designs

A major shift in the structural model of how trials are conducted — not just statistically but operationally — is represented by **decentralized clinical trials (DCTs)**. DCTs move some or all trial activities away from traditional clinical trial sites toward patients' homes or local healthcare facilities, leveraging digital health technologies.[^38]

### Fully Decentralized (Virtual) Trials
All activities occur away from traditional sites. Patients use remote eConsent, telehealth visits, ePRO/eCOA tools, wearable sensors, and direct-to-patient drug shipment. These trials are siteless and fully remote. While maximally patient-centric, they are practically limited to indications that do not require complex in-person procedures.[^39][^38]

### Hybrid Decentralized Trials
The most prevalent model in current practice, hybrid trials combine remote elements (telehealth, ePRO, wearables, home nursing) with in-person visits for complex assessments or interventions. The EMA, FDA, and ICH E6(R3) now formally recognize hybrid strategies. The global DCT market, valued at $9.63 billion in 2024, is projected to exceed $21 billion by 2030.[^40][^41][^42][^43]

### Key Digital Health Technology (DHT) Enablers
- Electronic Clinical Outcome Assessments (eCOA) and electronic Patient-Reported Outcomes (ePRO)
- Wearable sensors for continuous passive monitoring
- Telemedicine platforms for investigator-patient interactions
- Remote eConsent and electronic data capture (EDC)
- Mobile health (mHealth) applications

***

## Part V: Pragmatic and Embedded Trials

### Pragmatic Clinical Trials (PCTs)

Pragmatic trials are designed to inform real-world clinical decision-making rather than mechanistic research questions. They feature broad eligibility criteria (reflecting actual patient populations), routine care settings (rather than specialized research environments), flexible intervention delivery, and reliance on routinely collected electronic data. These designs maximize external validity — the degree to which findings apply to the broader population — at some cost to internal precision.[^44][^45]

### Embedded Pragmatic Clinical Trials (ePCTs)

Embedded PCTs are set entirely within routine healthcare delivery infrastructure, using existing EHR systems, clinical workflows, and healthcare registries to identify patients, deliver interventions, and collect outcome data. The NIH Pragmatic Trials Collaboratory has formally supported this model, which can dramatically reduce per-patient costs — median costs reported at approximately $97 per patient in published embedded PCTs. Key operational considerations include alignment with institutional IT capabilities, data sharing agreements, and potential waivers of informed consent for minimal-risk interventions.[^46][^45][^44]

### Registry-Based Randomized Clinical Trials (R-RCTs)

R-RCTs leverage existing national clinical quality registries — which already capture standardized patient data, treatment, and outcome information — as the infrastructure for a prospective randomized trial. Randomization modules are embedded into the registry workflow, and outcomes are ascertained from registry data rather than dedicated follow-up. This design is particularly well-suited for comparing approved therapies or devices already in clinical use, offering approximately 50% cost reduction over standard RCTs. R-RCTs are best regarded as complements to traditional RCTs rather than substitutes — they are suited to pragmatic, effectiveness questions rather than first-in-human or high-safety-concern studies.[^47][^48][^49][^50]

***

## Part VI: Synthetic and External Control Arms

Traditional randomized trials require concurrent control arms, which may be ethically or logistically infeasible in rare diseases, pediatric indications, or single-arm proof-of-concept settings. **External or synthetic control arms** use real-world data (RWD), historical clinical trial data, or statistical imputation to construct a comparator group — reducing or eliminating the need for a placebo or active comparator arm.[^51][^52]

- **Real-World Data (RWD) External Comparators**: Electronic health record (EHR)-derived cohorts are curated and matched to emulate the control arm of a clinical trial. The FDA has cited two landmark approvals leveraging this approach: Alecensa (2015, Roche) and Blincyto (Amgen) for rare leukemia.[^51]
- **Synthetic Control Arms (SCAs)**: Statistical methods (propensity score matching, IPTW, longitudinal matching) are applied to one or more RWD or historical trial data sources to construct a model-based control. Medidata AI's Synthetic Control Arm® platform is the first to combine cross-industry historical clinical trial data (HCTD) with RWD.[^53][^52]
- **Regulatory Status**: The FDA's 2025 draft guidance on CGT trials in small populations explicitly includes "externally controlled studies" as an accepted design. FDA cautions remain around temporal drift in treatment standards and analytic decision variability.[^54][^55][^40]

***

## Part VII: N-of-1 (Single-Patient) Trials

**N-of-1 trials** are multiple-period, crossover studies conducted in a single patient, who alternates between active treatment and control (or alternative treatments) across predefined washout periods. Rather than estimating population-average effects, they inform individualized treatment decisions for patients with chronic, stable conditions — directly embodying the principle of precision medicine. When pooled across multiple patients, N-of-1 trials can also generate population-level evidence. Applications include pain management, ADHD, chronic respiratory disease, and conditions where between-patient heterogeneity is high and within-patient effects are the primary interest.[^56]

***

## Part VIII: Stepped-Wedge Cluster Randomized Trials

**Stepped-wedge cluster randomized trials** (SW-CRTs) are a variant of cluster randomized designs in which all clusters (e.g., hospitals, clinics, schools) start in the control condition, then cross over to the intervention on a randomized, staggered schedule. By the trial's end, all clusters have received the intervention. Key advantages include:[^57][^58]
- Feasibility in contexts where simultaneous intervention rollout is logistically impossible
- Greater acceptability from communities and ethics boards (all will eventually receive the intervention)[^57]
- Within-cluster before-and-after comparisons that partially control for cluster heterogeneity

This design is particularly suited to health service evaluation, quality improvement interventions, and policy implementations. The primary statistical challenge is confounding between treatment effect and underlying secular (time) trends, which must be accounted for in the analysis.[^58][^59]

***

## Part IX: Digital Twins and In Silico Trials

**Digital twins** in clinical research refer to virtual patient representations constructed from individual physiological, genomic, and clinical data — enabling simulation of disease progression and treatment response in silico. This is one of the most nascent but potentially transformative design innovations:[^60][^61][^62]

- **In Silico Clinical Trials (ISCT)**: Replace or supplement trial arms with computationally generated virtual cohorts, validated against real clinical data.[^63][^61]
- **Synthetic/Virtual Control Arms via Digital Twins**: Companies such as Unlearn (using Neural-Boltzmann digital twins and PROCOVA) have demonstrated retrospectively that virtual twin approaches could reduce control arm sizes by up to 35% — with ZS Associates modeling a 60% potential reduction in control arms and 30% reduction in sample sizes as fidelity matures.[^64]
- **Current Applications**: Concentrated in rare diseases and high-mortality conditions; ZS Associates projects growing regulatory trust as model quality improves. PNAS Nexus (2025) highlights credibility assessment frameworks and AI-driven patient-specific simulation as key areas of methodological advancement.[^61][^62][^64]
- **Real-time Safety Monitoring**: Digital twins can enable early detection of adverse events through continuous in silico monitoring, improving patient safety.[^60]

***

## Part X: AI-Augmented and LLM-Integrated Trial Design

The most emergent category, **AI-driven trial design** integrates machine learning, reinforcement learning, and large language models (LLMs) across the trial lifecycle:[^65][^66]

- **Protocol Optimization**: AI models trained on historical trial data can predict outcomes with up to 85% accuracy, enabling prospective protocol refinement and preventing costly amendments. LLMs specifically are being applied to extract research elements, analyze trial termination patterns, optimize eligibility criteria, and evaluate the association between criteria complexity and termination risk.[^67][^66]
- **Reinforcement Learning for Real-Time Adaptation**: Reinforcement learning agents can manage trial adaptations in real time, dynamically adjusting dosing, randomization, or cohort inclusion based on continuously accumulating data.[^65]
- **Bayesian Adaptive Frameworks + AI**: Combining Bayesian adaptive designs with AI-driven simulation for operating characteristics validation represents the leading edge of hybrid design optimization.[^65]
- **Site Selection and Recruitment**: AI analyzes historical site performance to identify high-yield sites, with demonstrated enrollment improvement of up to 65% and 3x faster eligible patient identification.[^67]

Key regulatory challenges remain: AI-generated adaptive rules require pre-specification and regulatory review, and concerns about model interpretability, bias, and data privacy must be addressed before widespread regulatory acceptance.[^68][^65]

***

## Regulatory Framework and Trajectory

The FDA's Complex Innovative Trial Design (CID) Pilot Program and its 2024 final guidance on "Interacting with the FDA on Complex Innovative Trial Designs" provide structured pathways for sponsors to seek early feedback on master protocols, adaptive designs, and Bayesian methods. The ICH E6(R3) GCP guidance (finalized 2025) modernizes Good Clinical Practice to formally embrace risk-based approaches and a broader range of trial designs. The EMA's Regulatory Science to 2025 strategy similarly identifies innovative trial design as a strategic priority.[^3][^69][^40]

For CGT products in small/rare disease populations, FDA's 2025 draft guidance specifically endorses six design options: single-arm trials using participants as their own control, disease progression modeling, externally controlled studies, adaptive designs, Bayesian designs, and master protocol designs.[^54]

***

## Design Comparison Summary

| Design Type | Key Innovation | Best Use Case | Regulatory Maturity |
|---|---|---|---|
| Adaptive (general) | Pre-specified mid-trial modifications | Phase II/III dose, arm, and sample size decisions | High — FDA/EMA guidance exists |
| Response-Adaptive Randomization | Dynamic allocation favoring better arms | Multi-arm Phase II, rare diseases | Moderate — growing guidance |
| Seamless Phase 2/3 | Eliminates gap between phases | Oncology, targeted therapies | Moderate — complex to implement |
| Master Protocol – Basket | Common biomarker, multiple diseases | Oncology, rare disease | High — FDA guidance published |
| Master Protocol – Umbrella | One disease, multiple biomarker-matched therapies | Precision oncology | High |
| Platform Trial | Perpetual, add/drop arms | Pandemic response, chronic disease, oncology | High — landmark precedents |
| Bayesian | Prior + data updating inference | Rare disease, pediatrics, small samples | High — FDA adaptive design guidance |
| Decentralized/Hybrid | Patient home-based participation | PRO-heavy studies, chronic disease, accessibility | High — ICH E6(R3) recognizes |
| Pragmatic/Embedded | Routine care infrastructure | Comparative effectiveness, health system evaluation | Moderate |
| Registry-Based RCT | National registry as trial backbone | Comparing approved therapies | Moderate — growing in Scandinavia |
| Synthetic/External Control | RWD/historical data as control | Single-arm rare disease, CGT | Moderate — FDA case-by-case |
| N-of-1 | Crossover within a single patient | Chronic disease, personalized medicine | Low — no formal regulatory pathway |
| Stepped-Wedge Cluster | Staggered crossover of clusters | Health service evaluation, policy | Moderate |
| Digital Twin / In Silico | Computational virtual patients | Rare disease, CGT, device trials | Low-Moderate — emerging |
| AI/LLM-Augmented Design | Machine learning–driven protocol optimization | Protocol development, recruitment, adaptive rules | Low — no formal regulatory acceptance yet |

***

## Key Themes and Implications

**1. Convergence of Designs**: Modern trials increasingly combine multiple innovations simultaneously — e.g., a platform trial using Bayesian RAR, synthetic external controls, decentralized data collection, and biomarker enrichment. The modularity of design components enables powerful combinations.[^1][^4]

**2. Regulatory Enablement**: Regulatory agencies have moved from passive acceptance to active advocacy for innovative designs, particularly for rare diseases, CGT, and pandemic preparedness contexts. Sponsors are strongly encouraged to engage via scientific advice or the FDA's CID program early.[^3][^40][^54]

**3. Ethical Advantages**: Response-adaptive designs, platform trials, and N-of-1 designs all reduce patient exposure to inferior treatments — a critical ethical improvement over traditional parallel-group trials.[^4][^28]

**4. Data Infrastructure as a Prerequisite**: Innovative designs — especially platform trials, embedded PCTs, R-RCTs, and DCTs — require sophisticated data infrastructure: common data models, interoperable EDC platforms, electronic registries, and real-time statistical engines. Investment in this infrastructure is a prerequisite for design innovation.[^29][^49]

**5. Digital and AI Integration as the Frontier**: The integration of digital twins, LLMs, and reinforcement learning into trial design represents the next generation of innovation, with the potential to dramatically reduce costs, sample sizes, and timelines — but requires regulatory frameworks that have not yet fully materialized.[^64][^67][^65]

---

## References

1. [Innovative Trials - CTTI](https://ctti-clinicaltrials.org/innovative-trials/) - Innovative clinical trial designs that are more convenient for patients, can drive faster developmen...

2. [Adaptive Designs for Clinical Trials of Drugs and Biologics Guidance](https://www.fda.gov/regulatory-information/search-fda-guidance-documents/adaptive-design-clinical-trials-drugs-and-biologics-guidance-industry) - The guidance describes important principles for designing, conducting, and reporting the results fro...

3. [Interacting with the FDA on Complex Innovative Trial Designs for ...](https://www.fda.gov/regulatory-information/search-fda-guidance-documents/interacting-fda-complex-innovative-trial-designs-drugs-and-biological-products) - This document provides guidance to sponsors and applicants on interacting with the FDA on complex in...

4. [Recent innovations in adaptive trial designs: A review of ... - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC10260347/) - Clinical trials are constantly evolving in the context of increasingly complex research questions an...

5. [Insights into the adoption of innovative clinical trials across ... - Nature](https://www.nature.com/articles/s41598-025-18488-8) - The rise in innovative clinical trial designs reflects a shift toward addressing complex challenges ...

6. [Clinical trial design – keeping up with innovation | Journal](https://www.regulatoryrapporteur.org/pharmaceuticals/clinical-trial-design-keeping-up-with-innovation/574.article) - In drug development, novel trial designs have considerable potential which should be used to the ful...

7. [Adaptive design (medicine) - Wikipedia](https://en.wikipedia.org/wiki/Adaptive_design_(medicine))

8. [Key design considerations for adaptive clinical trials: a primer for clinicians](https://www.bmj.com/content/360/bmj.k698) - This article reviews important considerations for researchers who are designing adaptive clinical tr...

9. [Response adaptive randomisation in clinical trials: Current practice ...](https://pubmed.ncbi.nlm.nih.gov/40528416/) - <span><b>Introduction:</b> Adaptive designs (ADs) offer clinical trials flexibility to modify design...

10. [Response adaptive randomisation in clinical trials - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC12460923/) - Introduction: Adaptive designs (ADs) offer clinical trials flexibility to modify design aspects base...

11. [Response-adaptive randomization in clinical trials - PubMed](https://pubmed.ncbi.nlm.nih.gov/37324576/) - Response-Adaptive Randomization (RAR) is part of a wider class of data-dependent sampling algorithms...

12. [A Multi-Arm Two-Stage (MATS) design for proof-of-concept and ...](https://pubmed.ncbi.nlm.nih.gov/37419308/) - Following the spirit of Project Optimus, we propose an Multi-Arm Two-Stage (MATS) design for proof-o...

13. [FDA Project Optimus: Dose Optimization in Oncology Trials - Allucent](https://www.allucent.com/resources/blog/fda-project-optimus-dose-optimization-oncology-trials) - Explore how FDA Project Optimus is applied in practice, with insights on dose optimization, trial de...

14. [Project Optimus: The evolution of dose optimisation in oncology](https://www.fortrea.com/insights/project-optimus-evolution-dose-optimisation-oncology) - fortrea

15. [FDA's Project Optimus: A new era in oncology drug dosing](https://www.drugtargetreview.com/article/155573/fdas-project-optimus-a-new-era-in-oncology-drug-dosing/) - Discover how precision medicine and FDA's Project Optimus are transforming clinical trials in oncolo...

16. [FDA's Project Optimus: What Pharma and Biotech Need to Know](https://www.precisionformedicine.com/blog/fdas-project-optimus-what-pharma-and-biotech-need-to-know) - FDA launched Project Optimus as a transformative initiative aimed at reshaping how oncology drugs ar...

17. [On Enrichment Strategies for Biomarker Stratified Clinical Trials](https://dukespace.lib.duke.edu/server/api/core/bitstreams/86c5e3b0-c69e-46af-a093-1c0532290269/content)

18. [On Enrichment Strategies for Biomarker Stratified Clinical ...](https://pubmed.ncbi.nlm.nih.gov/28933670/) - In the era of precision medicine, drugs are increasingly developed to target subgroups of patients w...

19. [Phase III Precision Medicine Clinical Trial Designs That Integrate ...](https://pmc.ncbi.nlm.nih.gov/articles/PMC7446320/) - Recent advances in biotechnology and cancer genomics have afforded enormous opportunities for develo...

20. [Innovative Two-Stage Seamless Adaptive Clinical Trial Designs](https://researchopenworld.com/innovative-two-stage-seamless-adaptive-clinical-trial-designs/) - A two-stage seamless adaptive design in clinical research has become popular, which combines two sep...

21. [Adaptive Seamless Design For Phase 2/3 Studies: Basic Concepts ...](https://www.clinicalleader.com/doc/adaptive-seamless-design-for-phase-studies-basic-concepts-considerations-0001) - An adaptive design is a design that allows for modifications to the processes and statistical proced...

22. [An adaptive seamless Phase 2-3 design with multiple endpoints](https://pubmed.ncbi.nlm.nih.gov/33588655/) - Here we propose an adaptive seamless Phase 2-3 design with multiple endpoints which can expand an on...

23. [[PDF] Introduction to Seamless Trial Design](https://www.aacr.org/wp-content/uploads/2024/02/Session-3B-Slides.pdf) - Inferentially seamlessly adaptive Phase 2/3 designs, or adaptive Phase 2/3 designs for short, are hi...

24. [Practical Considerations and Recommendations for Master Protocol ...](https://pmc.ncbi.nlm.nih.gov/articles/PMC8220876/) - Master protocol, categorized as basket trial, umbrella trial or platform trial, is an innovative cli...

25. [[PDF] Master Protocols: Efficient Clinical Trial Design Strategies to ... - FDA](https://www.fda.gov/media/120721/download)

26. [E4H booklet Master protocol designs (Final)](https://ecrin.org/sites/default/files/2025-07/E4H%20booklet%20Master%20protocol%20designs.pdf)

27. [Platform Trials and Master Protocols: A New Approach to Clinical Research | Lilly Trials Blog](https://trials.lilly.com/en-US/blog/platform-trials-and-master-protocols) - New medicines are made possible by clinical research volunteers. Search to find a Lilly clinical tri...

28. [Platform Trials to Assess Therapeutics in Patients Hospitalized With ...](https://academic.oup.com/jid/article/232/Supplement_3/S254/8287896) - REMAP-CAP and RECOVERY are adaptive platform trials whose successes during the COVID-19 pandemic hol...

29. [April 2, 2021: Lessons from COVID-19: The First Year of the REMAP ...](https://rethinkingclinicaltrials.org/news/april-2-2021-lessons-from-covid-19-the-first-year-of-the-remap-cap-global-adaptive-platform-trial-derek-angus-md-mph/) - It is possible to design adaptive platform trials with a smaller sample size, depending on the resea...

30. [The European clinical research response to optimise treatment of ...](https://pmc.ncbi.nlm.nih.gov/articles/PMC8691848/) - The core design principle of both RECOVERY and REMAP-CAP trials is to facilitate integration of clin...

31. [GBM AGILE: Global Adaptive Trial Master Protocol: An International ...](https://www.yalemedicine.org/clinical-trials/a-trial-to-evaluate-multiple-regimens-in-newly-diagnosed-and-recurrent-glioblastoma) - GBM AGILE is an international, seamless Phase II/III response adaptive randomization platform trial ...

32. [NCT03970447 | A Trial to Evaluate Multiple Regimens in Newly ...](https://clinicaltrials.gov/study/NCT03970447) - GBM AGILE is an international, seamless Phase II/III response adaptive randomization platform trial ...

33. [[PDF] agile master platform protocol](https://www.agiletrial.net/wp-content/uploads/2025/03/AGILE_Master_Protocol_version_13.0_04_Sep_2024_clean.docx.pdf) - Protocol Information. This protocol describes the AGILE trial and provides information about procedu...

34. [The Bayesian Design of Adaptive Clinical Trials - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC7826635/) - This paper presents a brief overview of the recent literature on adaptive design of clinical trials ...

35. [Bayesian adaptive clinical trial designs for respiratory medicine - PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC9544135/) - An example of a Bayesian adaptive trial that incorporated arm dropping is the CATALYST study. This w...

36. [9 Bayesian Clinical Trial Design - hbiostat](https://hbiostat.org/bayes/bet/design) - A prototype Bayesian sequential design meeting multiple simultaneous goals will be discussed. The de...

37. [Complex Innovative Trials Designs: Current Perspectives & Future ...](https://www.berryconsultants.com/resource/complex-innovative-trials-designs--current-perspectives---future-directions) - Sheila Sprague (McMaster University) describe the launch and design of the Musculoskeletal Adaptive ...

38. [Decentralized clinical trials and digital health technologies - NCBI](https://www.ncbi.nlm.nih.gov/books/NBK609002/) - In hybrid DCTs, some trial activities involve in-person visits by trial participants to traditional ...

39. [Virtual, Decentralized & Hybrid Clinical Trials: What's the Difference?](https://medrio.com/blog/virtual-decentralized-hybrid-whats-the-difference/) - While the terms virtual and decentralized have been used synonymously, hybrid trials have also been ...

40. [Global Regulatory Updates on Clinical Trials (September 2025)](https://www.caidya.com/resources/regulatory-updates-sept-2025/) - , recommending novel trial designs and endpoints to support product licensure in small populations. ...

41. [Regulatory And Ethical...](https://www.mahalo.health/insights/decentralized-vs-hybrid-clinical-trials) - Explore decentralized and hybrid clinical trial models, essential tools, and best practices that enh...

42. [Decentralized Clinical Trials and Hybrid Monitoring](https://www.linical.com/articles-research/decentralized-clinical-trials-and-hybrid-monitoring-transforming-clinical-research) - While fully virtual trials offer flexibility, they may not suit all therapeutic areas or procedures....

43. [Decentralized Clinical Trials + Hybrid & Virtual Solutions](https://www.syneoshealth.com/solutions/clinical-development/decentralized-solutions) - Accelerate patient recruitment and retention with our decentralized and hybrid clinical trial soluti...

44. [The Embedded Pragmatic Clinical Trial Ecosystem - NIH Collaboratory](https://rethinkingclinicaltrials.org/chapters/design/what-is-a-pragmatic-clinical-trial/the-embedded-pct-ecosystemv2/) - To explore the potential of embedded pragmatic clinical trials and to establish best practices, the ...

45. [Pragmatic guidance for embedding pragmatic clinical trials in health ...](https://journals.sagepub.com/doi/abs/10.1177/17407745231160459) - The type of trial best suited for studies embedded in health plans will be those that require large ...

46. [Opportunities and barriers for pragmatic embedded trials - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC6508852/) - Embedded pragmatic clinical trials (PCTs) are set in routine health care, have broad eligibility cri...

47. [R-RCT - registry-based randomised clinical trials](https://www.ucr.uu.se/en/services/r-rct) - R-RCT are prospective randomised trials that use a clinical registry for one or several major functi...

48. [[PDF] Registry based randomized clinical trials (R-RCT) - EMA](https://www.ema.europa.eu/en/documents/presentation/presentation-registry-based-randomized-clinical-trials-r-rct-swedeheart-euroheart-l-wallentin-uppsala-university_en.pdf) - Contains individual standardized structured data on pa2ents, treatments, and outcomes. • Integrated ...

49. [Registry randomised trials: a methodological perspective - BMJ Open](https://bmjopen.bmj.com/content/13/3/e068057) - RRCTs can be embedded into large population-based registries or smaller single site registries to pr...

50. [The Registry-Based Randomized Trial - A Pragmatic Study Design](https://pubmed.ncbi.nlm.nih.gov/38320494/) - Randomized controlled trials are the gold standard of clinical research for comparing therapies in w...

51. [[PDF] Novel Applications of Real World Data (RWD) in External Control Arms](https://pharmasug.org/proceedings/2023/RW/PharmaSUG-2023-RW-324.pdf) - External control arms (ECA) may be sourced from prior clinical trial data (individual or pooled), or...

52. [The Pros And Cons Of Synthetic Control Arms In Clinical Trials](https://www.clinicalleader.com/doc/the-pros-and-cons-of-synthetic-control-arms-in-clinical-trials-0001) - Synthetic control arms (SCAs) are an innovative approach that is increasingly being adopted in clini...

53. [[PDF] Synthetic Control Arm®: The Regulatory Grade External ... - Medidata](https://www.medidata.com/wp-content/uploads/2024/08/Medidata-AI-Synthetic-Control-Arm-eBook-May-24.pdf) - This eBook provides guidance on the increasing role of ECAs, the differences between control groups ...

54. [FDA targets innovative CGT trial designs in draft guidance](https://www.clinicaltrialsarena.com/news/fda-targets-innovative-cgt-trial-designs-in-draft-guidance/) - The draft guidance sets out six alternative trial designs that sponsors could adopt in CGT studies, ...

55. [Pink Sheet - External Control Arms: Better Than Single-Arm Studies ...](https://friendsofcancerresearch.org/news/pink-sheet-external-control-arms-better-than-single-arm-studies-but-no-replacement-for-randomization/) - A synthetic control arm drawn from historical clinical trial data could provide better information a...

56. [Single-patient (n-of-1) trials: a pragmatic clinical decision ... - PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC3972259/) - Single-patient (n-of-1) trials are potentially useful for informing personalized treatment decisions...

57. [Stepped-Wedge Designs - Rethinking Clinical Trials](https://rethinkingclinicaltrials.org/chapters/design/experimental-designs-and-randomization-schemes/stepped-wedge-designs/) - In stepped-wedge designs, the clusters are randomized into several groups or waves that define when ...

58. [The stepped wedge cluster randomised trial: rationale, design ...](https://www.bmj.com/content/350/bmj.h391) - The design involves random and sequential crossover of clusters from control to intervention until a...

59. [Stepped-wedge cluster randomised controlled trials - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC4286109/) - Design and analysis of stepped wedge cluster randomized trials. ... cluster-randomized study design ...

60. [Enhancing randomized clinical trials with digital twins - PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC12494699/) - Digital twins (DTs) can transform randomized clinical trials by improving ethical standards, includi...

61. [The future of in silico trials and digital twins in medicine](https://pmc.ncbi.nlm.nih.gov/articles/PMC12043051/) - In silico trials and digital twins are emerging as transformative medical technologies, as they offe...

62. [future of in silico trials and digital twins in medicine | PNAS Nexus](https://academic.oup.com/pnasnexus/article/4/5/pgaf123/8116190) - Abstract. In silico trials and digital twins are emerging as transformative medical technologies, as...

63. [Digital Twin for Clinical Research and Development - Nova In Silico](https://www.novainsilico.ai/clinical-trial-simulation/digital-twin-for-clinical-research-and-development/) - Nova’s Jinkō platform creates virtual patient “twins” that mirror real patients’ characteristics and...

64. [Reimagining clinical trials with In Silico and AI-driven methods - ZS](https://www.zs.com/insights/true-value-potential-in-silico-clinical-development) - ZS quantifies how in-silico methodologies in drug development can reduce trial costs by 63% and shor...

65. [AI and innovation in clinical trials - PMC - NIH](https://pmc.ncbi.nlm.nih.gov/articles/PMC12627430/) - This perspective examines how artificial intelligence (AI), large language models (LLMs), adaptive t...

66. [Large language models in clinical trials: applications, technical ...](https://pmc.ncbi.nlm.nih.gov/articles/PMC12522288/) - As clinical trials scale up and grow more complex, researchers are facing mounting challenges, inclu...

67. [AI Clinical Trial Optimization: Revolutionary 2025 - Lifebit](https://lifebit.ai/blog/ai-clinical-trial-optimization-guide-2026/) - Revolutionize drug development with ai clinical trial optimization. Streamline trials, reduce costs,...

68. [AI In clinical trials in 2025: the edge of tech](https://clinicaltrialrisk.org/clinical-trial-design/ai-in-clinical-trials-the-edge-of-tech/) - AI plays a pivotal role in optimizing trial protocols by simulating various scenarios and predicting...

69. [[PDF] Innovation in Clinical Trial Design White Paper final - EFPIA](https://www.efpia.eu/media/547507/efpia-position-paper-innovation-in-clinical-trial-design-white-paper.pdf) - FACTS, emerging FDA regulatory guidance on Adaptive Designs for Clinical Trials of Drugs and. Biolog...

