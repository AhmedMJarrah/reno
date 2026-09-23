"""
reflection_decisions.py — case-by-case decisions for reflections the rules could not settle (reno v2.1).

Each decision was made by reading the amendment text, the previous version, the current snapshot and
the Diwan article version. reno_pipeline.py applies them with the same safety checks as the rules:
an operation that cannot find its anchor/old wording fails, and the case goes to the volunteer queue.

Actions
  ok    : the snapshot is already correct, or the amendment changes no article text
  vol   : needs a person (note says why)
  prev  : the snapshot belongs to another law -> rebuild it from the previous version, then apply "ops"
  ops   : apply operations to the current snapshot
Operations
  ("replace", art, old, new)        old -> new inside one article (no-op if already applied)
  ("delete", art, phrase)           remove a phrase (no-op if already absent)
  ("insert_after", art, anchor, t)  insert t after a unique anchor (no-op if t already present)
  ("append", art, text)             add text at the end of an article (no-op if already present)
  ("global", [(old, new), ...])     replace everywhere in the law
  ("restore", art)                  take this article from the previous version
  ("diwan", art, must_contain)      take the Diwan version of the article if it contains the new wording
"""

MANUAL = {
    # ---- already correct / no article text to change
    "6006": {"a": "ok", "note": "enforcement-date provision only"},
    "2837": {"a": "ok", "note": "art 8 already reads 'لا تزيد على سنتين'"},
    "5838": {"a": "ok", "note": "arts 5 and 9 already carry the new wording"},
    "2971": {"a": "ok", "note": "general provision (abolished posts), no textual edit"},
    "3092": {"a": "ok", "note": "amendment declared void (تم بطلانه)"},
    "1674": {"a": "ok", "note": "transitional provision, no textual edit"},
    "1745": {"a": "ok", "note": "approves an amendment to the annexed treaty, no article edit"},
    "782": {"a": "ok", "note": "art 8 already has new (ج) and renumbered (د)"},
    "1846": {"a": "ok", "note": "art 184 already without 'وغيرها'"},
    "5807": {"a": "ok", "note": "standalone 'على الرغم مما جاء' provision"},
    "4033": {"a": "ok", "note": "art 7 already reads الساكن في البناء / ساكنا في قسم"},
    "1900": {"a": "ok", "note": "art 7 already has the new members list"},
    "1947": {"a": "ok", "note": "art 15 already has new item 4"},
    "1958": {"a": "ok", "note": "fee schedule change, not an article"},
    "5477": {"a": "ok", "note": "fee schedule change, not an article"},
    "2003": {"a": "ok", "note": "art 4 already without the deleted phrase/paragraph"},
    "2018": {"a": "ok", "note": "snapshot already has the new fee wording"},
    "5005": {"a": "ok", "note": "amends the annexed concession agreement, not a law article"},
    "6075": {"a": "ok", "note": "standalone 'على الرغم مما ورد' provision"},
    "2209": {"a": "ok", "note": "'والخدمات' already added in arts 3, 6, 16"},
    "5989": {"a": "ok", "note": "art 26 already reads 'رقم 42 لسنة 1953'"},
    "2454": {"a": "ok", "note": "art 3 already names وزير الطاقة; other change is in the annexed agreement"},
    "2477": {"a": "ok", "note": "art 2(ب) already without the deleted phrase"},
    # ---- snapshot belongs to another law -> rebuild from the previous version
    "2647": {"a": "prev", "ops": [("global", [("ووزير التجارة", "ووزير الاقتصاد الوطني"), ("وزير التجارة", "وزير الاقتصاد الوطني"),
                                              ("وزارة التجارة", "وزارة الاقتصاد الوطني")])],
             "note": "omnibus renaming law; snapshot was another law's text"},
    "1710": {"a": "prev", "ops": [], "note": "snapshot was قانون دعاوى الحكومة; previous version already contains the added clause"},
    "3643": {"a": "prev", "ops": [("global", [("وزير التجارة - الجمارك", "وزير المالية"), ("وزير الجمارك والمكوس", "وزير المالية"),
                                              ("وزير التجارة والصناعة", "وزير المالية"), ("وزير التجارة والزراعة", "وزير المالية"),
                                              ("وزير التجارة", "وزير المالية"), ("وزير الجمارك", "وزير المالية"),
                                              ("وزارة التجارة - الجمارك", "وزارة المالية"), ("وزارة الجمارك والمكوس", "وزارة المالية"),
                                              ("وزارة التجارة والصناعة", "وزارة المالية"), ("وزارة التجارة والزراعة", "وزارة المالية"),
                                              ("وزارة التجارة", "وزارة المالية"), ("وزارة الجمارك", "وزارة المالية")])],
             "note": "omnibus renaming law; snapshot was another law's text"},
    # ---- precise edits
    "2849": {"a": "ops", "ops": [("delete", "7", "العربية")], "note": "delete 'العربية' from art 7(ج)"},
    "2872": {"a": "ops", "ops": [("delete", "12", "او بالبريد المسجل")], "note": "art 12(1)(أ)"},
    "2876": {"a": "ops", "ops": [("replace", "2", "والمستشار الحقوقي ومفتش العدلية", "وقضاة التشريع")], "note": ""},
    "1790": {"a": "ops", "ops": [("delete", "3", "بموافقة الملك")], "note": "art 3(ب)"},
    "1869": {"a": "ops", "ops": [("global", [("جامعة الزرقاء", "الجامعة الهاشمية")])], "note": "arts 2,4,5,6,7"},
    "6128": {"a": "ops", "ops": [("insert_after", "2", "الرمل", "الجبس")], "note": "art 2(ط)"},
    "4028": {"a": "ops", "ops": [("replace", "7", "الجمعية البلدية", "المجلس الاداري")], "note": "art 7 para 2"},
    "4140": {"a": "ops", "ops": [("global", [("وزارة المعارف", "وزارة التربية والتعليم"), ("وزير المعارف", "وزير التربية والتعليم")])],
             "note": "omnibus renaming law"},
    "2355": {"a": "ops", "ops": [("append", "35", "ب. لوزير الداخلية ان يستثني بصورة دائمة او مؤقتة اي بيان من البيانات التي تتضمنها "
                                                  "البطاقة الانتخابية الشخصية المنصوص عليها في الفقرة (ا) من هذه المادة بما في ذلك صورة الناخب.")],
             "note": "new art 35(ب); the electoral-districts schedule is not an article"},
    "3899": {"a": "ops", "ops": [], "must_contain": "تخصص جميع الرسوم والغرامات", "note": "new art 10 must be present"},
    "5339": {"a": "ops", "ops": [], "must_contain": "على مدعي الشفعة او الاولوية عند تقديم دعواه", "note": "art 2 new text must be present"},
    "2825": {"a": "ops", "ops": [("diwan", "41", "ومراقبة ما يقع على الشوارع من الاراضي المكشوفة")], "note": "art 41(أ)(1)"},
    "1512": {"a": "ops", "ops": [("restore", "1"), ("diwan", "4", "او فرض رسوم عن التصدير والاستيراد")],
             "note": "art 1 was the amendment's own title; art 4 items 6 and 8"},
    "3113": {"a": "ops", "ops": [("delete", "6", "من وزراء الدولة لشؤون رئاسة الوزراء ممارسة")], "note": "art 6(أ)"},
    # ---- need a person
    "3219": {"a": "vol", "note": "قانون الإعسار ألغى أحكام الإفلاس في قانون التجارة: يلزم تحديد المواد الملغاة يدوياً"},
    "2692": {"a": "vol", "note": "استبدال كلمات 'معهد/معاهد' حسب سياق النص - يحتاج قراراً بشرياً لكل موضع"},
    "5819": {"a": "vol", "note": "نص المادة 41 في النسخة السابقة هو نص تعديل آخر، لا نص المادة"},
    "2975": {"a": "vol", "note": "يلغي ثلاثة قوانين معدلة ويعيد النص الأصلي: يتطلب إرجاع نسخة سابقة"},
    "3063": {"a": "vol", "note": "نص التعديل غير موجود في البيانات"},
    "3066": {"a": "vol", "note": "نص التعديل ناقص (مادة السريان فقط)"},
    "3067": {"a": "vol", "note": "نص التعديل ناقص (مادة السريان فقط)"},
    "6038": {"a": "vol", "note": "نص التعديل ناقص (مادة السريان فقط)"},
    "6107": {"a": "vol", "note": "نص التعديل غير موجود في البيانات"},
    "1555": {"a": "vol", "note": "نص التعديل ناقص (مادة السريان فقط)"},
    "6119": {"a": "vol", "note": "الانعكاس لقانون آخر (قانون التعدين 1930) والتعديل على ذيل 1936"},
    "5113": {"a": "vol", "note": "الانعكاس لقانون آخر (تأجيل تنفيذ الديون) والتعديل متعدد المواد"},
    "6056": {"a": "vol", "note": "الانعكاس لقانون آخر وسلسلة التعديل مختلطة"},
    "2213": {"a": "vol", "note": "نص التعديل مطابق لنص 2209، ونص المادة 2 بالانعكاس هو نص تعديل"},
    "5061": {"a": "vol", "note": "قد يكون مكرراً مع 2045 (نفس التعديل بصيغتين)"},
    "2045": {"a": "vol", "note": "قد يكون مكرراً مع 5061 (نفس التعديل بصيغتين)"},
    "3658": {"a": "vol", "note": "تعديل واسع يمس نحو 30 مادة"},
    "4080": {"a": "vol", "note": "عدة تعديلات على مواد مختلفة منها استبدال كلمة في كل القانون"},
    "1989": {"a": "vol", "note": "استبدال تعريف داخل المادة 2 + جدول جديد"},
    "4895": {"a": "vol", "note": "تعديل قانون موازنة: إضافات لفقرات وفصول"},
    "2035": {"a": "vol", "note": "يلغي تعديلاً سابقاً (1936): يتطلب إرجاع نسخة سابقة"},
    "3152": {"a": "vol", "note": "إلغاء فقرة وإعادة ترقيم الفقرات (ج..ط)"},
    "2575": {"a": "vol", "note": "إلغاء الفقرة (ج) من المادة 7 وإعادة ترقيم - لم تُطبق"},
    "3999": {"a": "vol", "note": "يعدل قانون تعديل 1927 غير موجود كسجل مستقل"},
    "2504": {"a": "vol", "note": "تعديل أرقام مواد داخل النص (34 -> 37/38)"},
    "4969": {"a": "ok", "note": "art 3 already ends with the added clause"},
    # ---- added after the Diwan article export for 79 base laws (v2.3.1)
    "1625": {"a": "ok", "note": "standalone provisions (validity extension + prohibition), no textual edit"},
    "1630": {"a": "ok", "note": "arts 10/11 already have 'او مساحة'"},
    "1973": {"a": "ok", "note": "enforcement-date provision only"},
    "3050": {"a": "prev", "ops": [], "note": "snapshot was the amendment's own text; the law only extends a 1934 annex to the West Bank"},
    "2047": {"a": "ops", "ops": [("append", "8", "تحصّل اية نفقات سواء أأنفقت بعد اعطاء انذار ام بدون اعطائه ، وفاقاً لاحكام قانون جباية الضرائب لسنة 1935 .")],
             "note": "addition to the end of art 8"},
    "3049": {"a": "vol", "note": "يعدل ذيل قانون الصحة لسنة 1949 لكنه مربوط بسلسلة قانون الصحة، والانعكاس للذيل"},
    "2136": {"a": "vol", "note": "يلغي المواد 3-6 والانعكاس الحالي هو نص التعديل نفسه"},
}
