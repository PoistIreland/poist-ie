// ─── POIST.IE ASSESSMENT CONTENT BANK ─────────────────────────────────────
// Irish language proficiency screening — CEFR-referenced
// Content should be reviewed by a qualified Irish language professional
// before live deployment

const ASSESSMENT = {

  // ── PLACEMENT QUIZ ─────────────────────────────────────────────────────
  // 5 questions to route candidate to correct difficulty band
  placement: [
    {
      q: "Cad is brí le 'buíoch'?",
      opts: ["Feargach / Angry", "Buíoch / Grateful", "Tuirseach / Tired", "Sásta / Happy"],
      answer: 1
    },
    {
      q: "Líon an bhearna: Tá sí _____ bliain d'aois.",
      opts: ["fiche", "fichid", "fichead", "fiche déag"],
      answer: 2
    },
    {
      q: "Cén ceann acu seo atá ceart?",
      opts: [
        "Chuaigh mé go dtí an siopa inné",
        "Chuaigh mé ag an siopa inné",
        "Téigh mé go dtí an siopa inné",
        "Chuaigh mé don siopa inné"
      ],
      answer: 0
    },
    {
      q: "Cad é an brí atá le 'dá mbeadh airgead agam, cheannóinn carr'?",
      opts: [
        "Because I had money, I bought a car",
        "If I had money, I would buy a car",
        "Although I had money, I bought a car",
        "When I have money, I will buy a car"
      ],
      answer: 1
    },
    {
      q: "Roghnaigh an leagan is foirmiúla den abairt seo:",
      opts: [
        "Táim ag iarraidh labhairt leat faoin scéal",
        "Ba mhaith liom do chuid ama a iarraidh le dul i gcomhairle leat",
        "An féidir linn comhrá a bheith againn?",
        "Caithfimid caint faoi seo"
      ],
      answer: 1
    }
  ],

  // ── LISTENING MODULE ───────────────────────────────────────────────────
  // Audio generated via Abair.ie API in production
  listening: {
    A1_A2: {
      audioText: "Dia duit. Is mise Seán. Tá mé cúig bliana fichead d'aois. Tá mé i mo chónaí i gCorcaigh. Oibrím i siopa leabhar. Is maith liom léamh agus ceol.",
      label: "Seán ag cur síos air féin",
      hint: "Seán is introducing himself.",
      questions: [
        {
          q: "Cá bhfuil cónaí ar Sheán?",
          opts: ["Baile Átha Cliath", "Gaillimh", "Corcaigh", "Luimneach"],
          answer: 2
        },
        {
          q: "Cad é an post atá ag Seán?",
          opts: ["Múinteoir", "Oibríonn sé i siopa leabhar", "Dochtúir", "Feirmeoir"],
          answer: 1
        },
        {
          q: "Cad is maith le Seán?",
          opts: ["Spórt agus bia", "Taisteal agus ceol", "Léamh agus ceol", "Scannáin agus leabhair"],
          answer: 2
        }
      ]
    },
    B1_B2: {
      audioText: "Bhí cruinniú againn ar maidin le foireann na roinne. Pléadh ceisteanna tábhachtacha maidir le polasaí teanga na heagraíochta don bhliain seo chugainn. Aontaíodh go gcuirfí tús le clár oiliúna Gaeilge do gach ball foirne ón gcéad lá de mhí na Samhna. Beidh an clár ar fáil ar líne agus i láthair freisin.",
      label: "Achoimre ar chruinniú",
      hint: "A summary of a workplace meeting.",
      questions: [
        {
          q: "Cad a pléadh ag an gcruinniú?",
          opts: [
            "Buiséad na heagraíochta",
            "Polasaí teanga na heagraíochta",
            "Earcaíocht ball foirne nua",
            "Pleananna don Nollaig"
          ],
          answer: 1
        },
        {
          q: "Cathain a thosóidh an clár oiliúna?",
          opts: ["1 Meán Fómhair", "1 Deireadh Fómhair", "1 Samhain", "1 Nollaig"],
          answer: 2
        },
        {
          q: "Conas a bheidh an clár ar fáil?",
          opts: [
            "Ar líne amháin",
            "I láthair amháin",
            "Ar líne agus i láthair",
            "Trí chomhdháil físe"
          ],
          answer: 2
        }
      ]
    },
    C1_C2: {
      audioText: "Léiríonn an taighde is déanaí ó Ollscoil na Gaillimhe go bhfuil buntáistí suntasacha cognaíocha ag baint le hoideachas dátheangach. Tá scileanna réitigh fadhbanna agus solúbthacht mheabhrach níos fearr ag páistí a fhástar le dhá theanga. Ina theannta sin, tugann an taighde le fios go gcabhraíonn an dátheangachas le cosc ar mheath cognaíoch ag staid níos déanaí sa saol.",
      label: "Torthaí taighde ar an dátheangachas",
      hint: "Research findings on bilingualism.",
      questions: [
        {
          q: "Cad iad na buntáistí a luaitear don dátheangachas?",
          opts: [
            "Buntáistí sóisialta agus airgeadais",
            "Scileanna cognaíocha agus cosaint ar mheath meabhrach",
            "Deiseanna fostaíochta agus tuarastal níos airde",
            "Scileanna cumarsáide agus muinín"
          ],
          answer: 1
        },
        {
          q: "Cad a thugann an taighde le fios faoi dhaoine fásta?",
          opts: [
            "Go bhfoghlaimíonn siad teangacha níos tapúla",
            "Go gcabhraíonn an dátheangachas le cosc ar mheath cognaíoch",
            "Go bhfuil sé deacair teanga nua a fhoghlaim",
            "Go bhfuil tuirse teanga coitianta"
          ],
          answer: 1
        }
      ]
    }
  },

  // ── GAP-FILL READING MODULE ────────────────────────────────────────────
  // MCQ gap-fill — auto-scored
  reading: {
    A1_A2: {
      passage: "Is mise Máire. Tá mé i mo chónaí ___ Gaillimh. Tá mé ag obair ___ múinteoir i scoil áitiúil. Tá beirt pháistí ___. Is maith liom ___ ag siúl cois farraige.",
      blanks: [
        { pos: 0, opts: ["ar", "i", "ag", "le"], answer: 1, hint: "location" },
        { pos: 1, opts: ["mar", "ag", "le", "ina"], answer: 0, hint: "role" },
        { pos: 2, opts: ["agam", "agat", "aige", "aici"], answer: 0, hint: "possession" },
        { pos: 3, opts: ["dul", "bheith", "ag dul", "an dul"], answer: 2, hint: "verbal noun" }
      ]
    },
    B1_B2: {
      passage: "Tá athrú mór ___ ar an margadh fostaíochta in Éirinn le roinnt blianta anuas. ___ go bhfuil suas le daichead faoin gcéad d'fhostóirí sa earnáil phoiblí ag lorg iarrthóirí ___ cumas Gaeilge acu. ___ an t-éileamh seo de bharr Acht na dTeangacha Oifigiúla 2021.",
      blanks: [
        { pos: 0, opts: ["tagtha", "tar éis", "ag teacht", "tiocfaidh"], answer: 0, hint: "perfect tense" },
        { pos: 1, opts: ["Deirtear", "Deir", "Ráitear", "Luadh"], answer: 0, hint: "reported speech" },
        { pos: 2, opts: ["a bhfuil", "go bhfuil", "nach bhfuil", "ina bhfuil"], answer: 0, hint: "relative clause" },
        { pos: 3, opts: ["Eascraíonn", "Tagann", "Tógtar", "Méadaíonn"], answer: 0, hint: "causation" }
      ]
    },
    C1_C2: {
      passage: "Is cuid ___ de bheartas teanga na hÉireann é Acht na dTeangacha Oifigiúla, ach maíonn criticeóirí go bhfuil bearnaí ___ ann i gcur i bhfeidhm na reachtaíochta. Cé go gcuireann an tAcht oibleagáid ___ ar chomhlachtaí poiblí, níl socrú ___ déanta chun na hiarrthóirí cáilithe a aimsiú.",
      blanks: [
        { pos: 0, opts: ["lárnach", "tábhachtach", "riachtanach", "bunúsach"], answer: 0, hint: "adjective — central" },
        { pos: 1, opts: ["suntasacha", "móra", "tromchúiseacha", "soiléire"], answer: 0, hint: "adjective — notable" },
        { pos: 2, opts: ["dhlíthiúil", "dhaingean", "láidir", "cinnte"], answer: 0, hint: "legal obligation" },
        { pos: 3, opts: ["leordhóthanach", "sásúil", "oiriúnach", "cuí"], answer: 0, hint: "sufficient" }
      ]
    }
  },

  // ── READING ALOUD / SPEAKING MODULE ───────────────────────────────────
  // Candidate reads passage aloud — recorded and stored for employer review
  speaking: {
    A1_A2: {
      passage: "Is mise [d'ainm]. Tá mé i mo chónaí i [do chontae]. Tá mé ag obair mar [do phost]. Taitníonn Gaeilge liom go mór agus bainim úsáid aisti gach lá más féidir liom.",
      instruction: "Read this passage aloud clearly and at a natural pace. Replace the words in brackets with your own information.",
      tip: "Take your time. Employers are listening for your pronunciation and natural fluency, not perfection.",
      duration: 45
    },
    B1_B2: {
      passage: "Tá ról tábhachtach ag an nGaeilge i mo shaol gairmiúil. Creideann mé go láidir gur féidir le cainteoirí Gaeilge luach ar leith a chur le heagraíochtaí poiblí, go háirithe agus an sprioc 2030 de chuid Acht na dTeangacha Oifigiúla le comhlíonadh ag na heagraíochtaí sin.",
      instruction: "Read this passage aloud clearly. Focus on natural rhythm and pronunciation.",
      tip: "Employers want to hear how you sound in a professional context — imagine you are reading a report at a team meeting.",
      duration: 60
    },
    C1_C2: {
      passage: "Léiríonn staitisticí le déanaí go bhfuil ganntanas gearrthéarmach ann maidir le cainteoirí Gaeilge inniúla sa tseirbhís phoiblí. Ní hamháin go gcruthaíonn sé seo dúshláin d'eagraíochtaí atá ag iarraidh a n-oibleagáidí reachtúla a chomhlíonadh, ach cuireann sé le caillteanas deiseanna do chainteoirí líofa atá ag lorg post ina n-úsáidfear a gcumas teanga.",
      instruction: "Read this passage aloud at a natural pace. This is a formal text — reflect that in your delivery.",
      tip: "At C1/C2 level, employers are listening for confident, fluent delivery with appropriate stress and intonation.",
      duration: 90
    }
  },

  // ── WRITING MODULE ─────────────────────────────────────────────────────
  // Short task — AI scored via Claude API
  writing: {
    A1_A2: {
      prompt: "Scríobh cúig abairt faoi tú féin as Gaeilge.\n\nWrite five sentences about yourself in Irish. Include your name, where you live, your job or studies, and one thing you enjoy.",
      target: "40–80 words",
      example: "e.g. Is mise Síle. Tá mé i mo chónaí i gCorcaigh...",
      scoringFocus: "Basic vocabulary, simple present tense (Tá/Is), personal pronouns, basic sentence structure"
    },
    B1_B2: {
      prompt: "Scríobh ríomhphost gearr chuig fostóir.\n\nWrite a short professional email (80–120 words) to a potential employer expressing your interest in an Irish language role. Mention your Irish level and one reason why you want to work in an Irish-speaking environment.",
      target: "80–120 words",
      example: "e.g. A [ainm] a chara, Scríobhim chugat maidir le...",
      scoringFocus: "Formal register, appropriate greeting and sign-off, varied vocabulary, correct verbal noun usage, basic conditional forms"
    },
    C1_C2: {
      prompt: "Scríobh alt gearr (120–180 focal) ag tabhairt do thuairim faoin ráiteas seo:\n\n'Tá dualgas ar gach eagraíocht phoiblí in Éirinn seirbhísí Gaeilge a chur ar fáil, ach níl na hacmhainní riachtanacha ar fáil chun é sin a dhéanamh i gceart.'\n\nGive your view with at least two reasons.",
      target: "120–180 words",
      example: "e.g. Is ceist chasta í seo a bhfuil dhá thaobh léi...",
      scoringFocus: "Cohesive argument, complex subordinate clauses, subjunctive mood, abstract vocabulary, academic register, appropriate hedging language"
    }
  },

  // ── CEFR SCORING ───────────────────────────────────────────────────────
  scoring: {
    thresholds: [
      { min: 0,  max: 24,  level: "A1", label: "Beginner" },
      { min: 25, max: 39,  level: "A2", label: "Elementary" },
      { min: 40, max: 54,  level: "B1", label: "Intermediate" },
      { min: 55, max: 69,  level: "B2", label: "Upper-Intermediate" },
      { min: 70, max: 84,  level: "C1", label: "Advanced" },
      { min: 85, max: 100, level: "C2", label: "Proficient" }
    ],
    borderlineMargin: 4,
    weights: { listening: 0.30, reading: 0.30, writing: 0.25, speaking: 0.15 }
  },

  // ── CREDENTIAL MAPPING ─────────────────────────────────────────────────
  credentials: [
    { label: "Leaving Cert Higher H1",    indicative: "C1" },
    { label: "Leaving Cert Higher H2",    indicative: "C1" },
    { label: "Leaving Cert Higher H3",    indicative: "B2" },
    { label: "Leaving Cert Higher H4",    indicative: "B2" },
    { label: "Leaving Cert Higher H5",    indicative: "B1" },
    { label: "Leaving Cert Ordinary O1",  indicative: "B1" },
    { label: "Leaving Cert Ordinary O2",  indicative: "A2" },
    { label: "TEG B2 certificate",        indicative: "B2", verified: true },
    { label: "TEG C1 certificate",        indicative: "C1", verified: true },
    { label: "TEG C2 certificate",        indicative: "C2", verified: true },
    { label: "Gaeltacht native speaker",  indicative: "C2" },
    { label: "Gaelscoil / Gaelcholáiste", indicative: "B1" }
  ]
};

window.ASSESSMENT = ASSESSMENT;
