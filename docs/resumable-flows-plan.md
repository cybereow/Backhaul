# پلن: flow‌های resumable روی wsmux/wssmux

وضعیت: پیشنهاد؛ فاز ۰ (اندازه‌گیری) انجام شد و سؤال‌های باز جواب گرفتند (بخش ۸ و ۱۰). هنوز کدی زده نشده. نویسنده: Claude. مبنا: بررسی کد در main (`f4303a8`) و آزمایش‌های اندازه‌گیری روی loopback. نسخه‌ی دوم: بعد از ریویو، اصلاح‌ها در بخش ۹ خلاصه شده‌اند.

## ۱. مسئله

یک flow (مثلاً SSH) روی یک stream از یک smux session سوار است. smux نمی‌تواند stream را به session دیگر ببرد، پس flow حداکثر تا سقف سخت CDN (`L`) زنده می‌ماند. rotation فعلی (`cdn_max_age`، retire در حدود `0.7L` با jitter ±۱۰٪ و drain تا `L`) فقط جلوی stream جدید روی session پیر را می‌گیرد؛ در آزمایش، عمر متوسط flow بلند از ۱۴s (بدون rotation) به ۲۱s رسید (`L=30`) و با ضریب کمتر تا ~۹۰٪ `L`. **هیچ تنظیمی عمر را از `L` بیشتر نمی‌کند.**

## ۲. دو سطح راه‌حل، یک پایه‌ی مشترک

| | سطح A: انتقال برنامه‌ریزی‌شده | سطح B: resume بعد از قطع ناگهانی |
|---|---|---|
| وقتی | در سن rotation، stream قدیمی هنوز سالم است | session بی‌خبر مرد (idle timeout، قطعی شبکه، deploy CDN) |
| نیاز به بافر replay | خیر (stream قدیمی بایت‌های در راه را تحویل می‌دهد) | بله (بایت‌های ack‌نشده) |
| سربارِ همیشگی | envelope (۳ بایت در هر رکورد ≤۳۲KiB + یک کپی) | + بافر حافظه + ack |
| نتیجه | flow از `L` عبور می‌کند | flow از قطع ناگهانی pool session هم جان سالم به در می‌برد |

**تصمیم طراحی کلیدی: هر دو سطح روی یک قالب داده و یک پروتکل attach ساخته می‌شوند.**

- **قالب داده از روز اول یکی است:** flow‌های resumable از همان باز شدن داخل envelope موجود half-close (`handlers.NewHalfCloseConn`: رکوردهای `DATA/END/ABORT`) حرکت می‌کنند. دلیل: قالب payload یک stream در لحظه‌ی باز شدن flow تعیین می‌شود و بعداً عوض نمی‌شود. اگر A روی stream خام بیاید و B بعداً فریم‌بندی بخواهد، flow‌هایی که با A باز شده‌اند هرگز resumable نمی‌شوند و هدر flow باید دوباره عوض شود؛ یعنی همان بازنویسی که می‌خواهیم از آن فرار کنیم. envelope همین الان پایان صریح (`END`) دارد، که لازم است: در smux قطع session می‌تواند به شکل `io.EOF` در `Read` ظاهر شود (خطای socket زیرین)، پس بدون `END` صریح نمی‌شود «پایان تمیز» را از «قطع» تشخیص داد. B فقط یک نوع رکورد `ACK` به envelope اضافه می‌کند.
- **attach آفست‌دار با فیلد `mode`:** در A آفست‌ها از freeze معلوم‌اند و stream قدیمی drain می‌شود؛ در B از آفست دریافتی طرف مقابل + replay. هدر از روز اول هر دو آفست را دارد (بخش ۴.۲).

## ۳. چیزهایی که از قبل داریم

- `PumpSwapper` (`internal/utils/handlers/promotable_pump.go`): freeze جهت upload در مرز نوشتن، تبادل count دو طرف (`exchangeCounts`)، نصب tunnel جدید، drain دقیقِ `dlLimit` بایت از tunnel قدیمی.
  - شمارنده‌های `upBytes/dlBytes` در حال حاضر **آفست قابل اتکا نیستند**: برای آمار/تصمیم promotion‌اند، و در فاز ۲ پمپ `n` (نه بایت تحویل‌شده) اضافه می‌شود. آفست‌های واقعی `total` محلی هر پمپ است که **نسبت به ابتدای tunnel فعلی** شمرده می‌شود، نه ابتدای flow.
- کلاینت: `promotableFlows` (flowID به swapper) و `handlePromoteStream`. این map در `restart` (افت کامل generation) خالی می‌شود و flow‌ها با `ctx` همان generation abort می‌شوند.
- سرور: `dispatchPromotable`، `promoteFlow`، registry جلسه‌ها، `awaitReplacement`/`retireSession`.
- envelope half-close (`FlowPlainHC = 0x06`) و الگوی capability (`halfclose-v1`) روی upgrade.
- محدودیت‌ها:
  - `PumpSwapper` یک‌بار-مصرف است (`installed`، `old` تغییرناپذیر، `phases` از ۲ فقط یک بار پایین می‌آید).
  - promotable فقط وقتی است که `mux_version >= 2` و `promote_bytes > 0` و `legsPerFlow() > 1`؛ در حالت plain خالص هیچ flow‌ای از `PumpSwapper` رد نمی‌شود.
  - flowID الان `time.Now().UnixNano()` است و غیرصفر بودنش به کلاینت می‌گوید «promotable».
  - promotable‌ها full-close‌اند و envelope ندارند؛ و بعد از EOF یک جهت (`ended`) دیگر قابل انتقال نیستند.
  - `Promote` اول freeze می‌کند و بعد پاسخ طرف مقابل را می‌خواند؛ پس هر رد شدن از طرف مقابل (flow ناشناخته، تمام‌شده، نسخه‌ی قدیمی) بعد از freeze است و flow را abort می‌کند.

## ۴. طراحی

### ۴.۱ مدل flow
هر flow resumable یک `flowID` (۶۴ بیتی تصادفی از `crypto/rand`، غیرصفر، نه زمان‌محور) و دو آفست یکنواخت دارد که **از ابتدای flow** شمرده می‌شوند و در برابر swap و resume ثابت می‌مانند:
- `sent`: بایت‌های payload که در tunnel commit شده‌اند،
- `recv`: بایت‌های payload که به app تحویل شده‌اند.

`PumpSwapper` برای هر tunnel یک `base` نگه می‌دارد (آفست در لحظه‌ی نصب آن tunnel) و `limit` محلی = `peerSent − base`. هدر PROXY protocol (که سرور قبل از پمپ روی اولین tunnel می‌نویسد) خارج از فضای آفست است و هرگز replay نمی‌شود.

### ۴.۲ پروتکل attach (flow kind جدید، `FlowAttach = 0x07`)
دو مرحله، تا رد شدن طرف مقابل **قبل** از freeze رخ دهد:

1. سرور: `kind | flowID(8) | mode(1) | flags(1)` روی stream جدید (داخل envelope).
2. کلاینت: `ACCEPT` یا `REJECT(reason)`. `REJECT` (flow ناشناخته، تمام‌شده، در حال swap، mode پشتیبانی‌نشده) یعنی flow روی stream قدیمی می‌ماند و stream جدید بسته می‌شود؛ **بدون abort**.
3. فقط بعد از `ACCEPT` هر دو طرف freeze می‌کنند و `mySent(8) | myRecv(8)` را رد و بدل می‌کنند (جایگزین `exchangeCounts` فعلی که فقط یک عدد دارد).

- `mode = drained` (سطح A): از `peerSent` استفاده می‌شود؛ هر طرف stream قدیمی را تا `recv == peerSent` drain می‌کند. `peerRecv` فقط sanity check است (`peerRecv <= mySent`). **`recv` در لحظه‌ی attach نهایی نیست** (بایت‌ها هنوز روی stream قدیمی در راه‌اند)، پس A نمی‌تواند فقط با `recv` کار کند؛ برای همین هر دو آفست ارسال می‌شود.
- `mode = resume` (سطح B): از `peerRecv` استفاده می‌شود؛ هر طرف از `peerRecv` به بعد را از بافرش replay می‌کند. اگر `peerRecv` زیر ابتدای بافر باشد → `REJECT` و flow بسته می‌شود.
- `flags`: رزرو (مثلاً برای B) تا افزودن قابلیت بعدی هدر را عوض نکند.
- فقط سرور stream باز می‌کند (OpenStream سمت سرور است)، پس attach همیشه سرور-آغازگر است و race دوطرفه‌ی «دو attach همزمان» نداریم. ولی attach با flow‌ای که همزمان در حال تمام شدن است race دارد؛ کلاینت باید swapper را **قبل** از شروع پمپ‌ها در map ثبت کند (الان بعد از `PromotablePump` ثبت می‌شود) و حالت «تمام‌شده» را با `REJECT` گزارش دهد.
- **سازگاری (تصمیم مالک پروژه):** پروژه شخصی است و نسخه‌ی پایدار منتشرشده‌ای ندارد؛ پس نه negotiation capability (`resume-v1/v2`) لازم است و نه تست rolling upgrade. هدر و رفتار flow مستقیماً عوض می‌شوند و سرور و کلاینت باید با هم آپدیت شوند. (اگر روزی نسخه‌ی پایدار منتشر شد، capability روی همان هدر اضافه می‌شود: فیلد `flags` برای همین رزرو شده است.)

### ۴.۳ سطح A
1. **`PumpSwapper` چندبارمصرف:** `old` تغییرپذیر می‌شود (زیر `mu`)، بعد از هر swap tunnel جدید `old` می‌شود، `base` آپدیت می‌شود و state machine (`freezeReq/frozen/installed/upAck/installCh/phases`) برای swap بعدی ریست می‌شود. هر جهت بعد از switch باید دوباره همان مسیر freeze-aware فاز ۱ را اجرا کند (الان فاز ۲ یک حلقه‌ی ساده‌ی بدون freeze است). چون فقط `net.Conn` می‌بیند، flow‌های promote‌شده به گروه striped هم پوشش داده می‌شوند.
2. **half-close:** flow‌ای که یک جهتش `END` شده هنوز باید قابل انتقال باشد (مثلاً درخواست تمام شده و دانلود بلند ادامه دارد). یعنی `ended` فعلی که هر انتقالی را می‌بندد باید به «این جهت تمام شد» تبدیل شود: جهت تمام‌شده در freeze فوراً ack می‌دهد و روی tunnel جدید فقط `END` می‌فرستد.
3. **orchestrator:** وقتی `awaitReplacement` true برگرداند، برای هر flow روی session در حال بازنشستگی: یک stream روی **جوان‌ترین session سالم** (نه لزوماً همان replacement؛ placement سن‌محور موجود) باز می‌شود و attach با `mode=drained` و `Promote` با build هویتی. همزمانی محدود (مثلاً ۸ انتقال همزمان) تا handshake‌ها با هم timeout نخورند.
4. **بودجه‌ی زمان:** انتقال باید قبل از `L` تمام شود: handshake (`PromoteHandshakeTimeout` = ۱۰s) + drain بایت‌های در راه روی stream قدیمی. drain به سرعت خواندن app بستگی دارد (app کند ⇒ پنجره‌ی smux پر ⇒ drain طولانی). پس:
   - انتقال‌ها بلافاصله بعد از retire شروع شوند (نه آخر پنجره)،
   - اگر `awaitReplacement` تا نزدیک `L` جایگزین پیدا نکرد (re-ask هر ~۳۰s است)، به جای انتظار، flow‌ها به هر session سالم دیگری منتقل شوند،
   - اگر `L - age` کمتر از بودجه‌ی لازم است، انتقال اصلاً شروع نشود (شکست بعد از freeze بدتر از قطع در `L` نیست ولی diag event شلوغ می‌کند).
5. **`retireSession`:** الان با `NumStreams()` و polling درین می‌کند؛ stream جدید flow روی session دیگر است پس منطق موجود کار می‌کند، فقط باید سقف drain (`MaxDrain = L − age`) با زمان انتقال هماهنگ باشد.
6. شکست قبل از `ACCEPT`/freeze بی‌خطر است (flow روی stream قدیمی می‌ماند). شکست بعد از freeze flow را abort می‌کند.

### ۴.۴ سطح B (فاز بعد، روی همان پروتکل و همان envelope)
- **رکورد `ACK(off)`** به envelope اضافه می‌شود (نوع `0x04`، طول ۸). `END` و `ABORT` موجودند؛ قطع بدون `END/ABORT` یعنی «تعلیق»، نه پایان.
- **بافر replay:** حلقه‌ی `K` بایتی **فقط برای جهت ارسالی هر طرف** (مثلاً ۱–۴ MiB، متناسب با BDP). وقتی ack‌نشده به `K` برسد sender block می‌شود (backpressure مثل TCP). حافظه‌ی هر process = `تعداد flow × K` (سرور بافر download را نگه می‌دارد، کلاینت بافر upload را)، پس یک سقف سراسری لازم است؛ flow‌های بیش از بودجه فقط سطح A می‌گیرند (بدون بافر، `resume` رد می‌شود).
- **تعلیق و resume:** هر دو طرف state را تا `resume_window` نگه می‌دارند؛ socket محلی باز می‌ماند و app فقط توقف می‌بیند. سرور روی یک session زنده stream جدید با `mode=resume` باز می‌کند. پس از پایان پنجره، flow بسته می‌شود.
- **عمر flow از generation جدا می‌شود:** الان `PromotablePump` با `g.ctx` ساخته می‌شود و `restart` کلاینت (مثلاً وقتی control channel در پنجره‌ی grace برنگشت) map را خالی و همه‌ی flow‌ها را abort می‌کند. B بدون جدا کردن state flow از generation فقط مرگ یک pool session را پوشش می‌دهد، نه restart. محدوده‌ی پیش‌فرض: فقط مرگ pool session؛ تحمل restart در صورت نیاز در فاز ۳ جدا تصمیم‌گیری شود.
- ack هر N بایت یا هر T میلی‌ثانیه (piggyback روی DATA جهت مخالف وقتی ممکن است).

### ۴.۵ پیکربندی
هدف: **بدون knob جدید برای A.** با `cdn_max_age > 0` و `mux_version >= 2` و capability `resume-v1` از هر دو طرف، A خودکار فعال می‌شود. این یعنی flow‌ها مستقل از `promote_bytes`/`legsPerFlow` از مسیر `PumpSwapper` + envelope رد می‌شوند (با ریسک هزینه‌ای که فاز ۰ می‌سنجد). برای B حداکثر یک knob: `resume_window` (ثانیه، `0` = خاموش). بودجه‌ی حافظه مقدار پیش‌فرض داخلی دارد.

تداخل با `mux_half_close`: چون flow‌های resumable از قبل envelope دارند، رفتار half-close را خودبه‌خود دارند؛ `mux_half_close` فقط برای flow‌های غیر-resumable معنا دارد.

## ۵. فازها و معیار پذیرش

**فاز ۰: اندازه‌گیری (اسپایک، بدون merge).** هزینه‌ی مسیر `PromotablePump` **+ envelope** برای همه‌ی flow‌ها در برابر مسیر فعلی (`io.Copy`/splice) با `e2e/run.sh` و `e2e/syscalls.sh`. اگر بیش از آستانه بود (پیشنهاد: افت throughput > ۵٪ یا syscall/byte > ۱۰٪)، گزینه‌ی «فقط flow‌هایی که از سن مشخصی گذشتند» بررسی می‌شود؛ اما توجه: envelope باید در لحظه‌ی باز شدن flow تصمیم گرفته شود، پس این گزینه فقط پمپ را تأخیری می‌کند، نه envelope را.

**فاز ۱: `PumpSwapper` چندبارمصرف + آفست از ابتدای flow + انتقال بعد از END یک جهت.** معیار: تست property-based (بایت‌های تصادفی، تعداد swap تصادفی، قطع تصادفی بین مراحل، END تصادفی یک جهت) بدون گم‌شدن/تکرار؛ `-race` تمیز؛ تست‌های موجود promotion بدون تغییر سبز.

**فاز ۲: سطح A.** `FlowAttach` با `ACCEPT/REJECT` + capability + orchestrator + ادغام با `retireSession`. معیار: آزمایش proxy کشنده‌ی سن (همان هارنس قبلی، تبدیل به تست e2e): flow بلند ≥ ۵ برابر `L` زنده بماند و بایت‌ها verify شوند (هم flow پرحجم، هم flow بیکار مثل SSH، هم flow نیمه‌بسته)؛ هیچ flow کوتاهی fail نشود؛ `REJECT` (flow ناشناخته/تمام‌شده) هرگز flow را abort نکند.

**فاز ۳: سطح B.** رکورد ACK، بافر، تعلیق/resume، بودجه‌ی حافظه. معیار: تست chaos که pool connection‌ها را در سن تصادفی (و بدون اخطار) می‌کشد؛ flow‌ها سالم و بایت‌به‌بایت درست می‌مانند؛ سقف حافظه رعایت می‌شود؛ throughput با `resume_window = 0` نسبت به فاز ۲ تغییر نمی‌کند.

**فاز ۴:** README/config، مهاجرت، تست سازگاری rolling upgrade در هر چهار ترکیب (سرور قدیم/جدید × کلاینت قدیم/جدید).

## ۶. ریسک‌ها

| ریسک | کاهش |
|---|---|
| باگ در state machine swap (گم‌شدن/تکرار بایت) | property/fuzz test قبل از هر چیز؛ فاز ۱ جدا و مستقل merge شود |
| سربار مسیر swapper + envelope برای همه‌ی flow‌ها | فاز ۰ قبل از تعهد |
| abort شدن flow در شکست بعد از freeze | `ACCEPT` قبل از freeze؛ بقیه محدود به زمانی که قبلاً هم در `L` قطع می‌شد؛ شمارنده/diag event |
| drain کند (app کند) بعد از `L` تمام شود | شروع فوری انتقال بعد از retire، شروع نکردن وقتی بودجه‌ی زمان کافی نیست |
| جایگزین دیر برسد (`awaitReplacement` بی‌انتها منتظر است) | انتقال به هر session سالم دیگر |
| حافظه‌ی بافر در سطح B | سقف سراسری + fallback به سطح A |
| restart کامل کلاینت همه‌ی flow‌ها را می‌کشد | خارج از محدوده‌ی پیش‌فرض B (بخش ۴.۴) |
| سازگاری با نسخه‌ی قدیمی | capability `resume-v1`/`resume-v2`، فقط وقتی هر دو طرف اعلام کنند. دلیل لازم بودن: کلاینت قدیمی `0x07` را نمی‌شناسد و stream را می‌بندد |
| تداخل با half-close (`FlowPlainHC`) | resumable‌ها از ابتدا envelope دارند؛ فاز ۱ انتقال بعد از END را پوشش می‌دهد |
| flow‌های UDP | خارج از دامنه |

## ۷. خارج از دامنه
UDP flow‌ها، ws/wss ساده (هر flow یک connection اختصاصی دارد و ذاتاً به `L` محدود است)، restart کامل process/generation، و تغییر در smux.

## ۸. سؤال‌های باز: تصمیم‌ها
1. **سطح B:** طراحی می‌شود؛ ساختش بعد از فاز ۲ و بر اساس دیدن قطع‌های ناگهانی در عمل. (تأیید شد)
2. **حافظه‌ی replay:** `K = 4 MiB` برای هر flow قابل قبول است ولی مصرف منابع باید کم بماند؛ پس سقف سراسری داخلی و فقط برای flow‌های فعال لازم است.
3. **دامنه:** همه‌ی flow‌ها resumable شوند.
4. **آستانه‌ی عملکرد:** هدف مطلق ۹۵۰ Mbps دانلود و ۹۵۰ Mbps آپلود (نه نسبت درصدی). نتیجه‌ی فاز ۰ در بخش ۱۰.
5. **سازگاری با نسخه‌های قدیمی:** لازم نیست (بخش ۴.۲).

## ۹. تغییرات نسبت به نسخه‌ی اول (نتیجه‌ی ریویو)
1. **ادعای «B فقط بافر و ack اضافه می‌کند» بدون قالب داده‌ی مشترک درست نبود:** فریم‌بندی B در لحظه‌ی باز شدن flow تعیین می‌شود. حالا flow‌های resumable از اول envelope موجود half-close را دارند و B فقط رکورد `ACK` اضافه می‌کند. envelope همچنین `END` صریح می‌دهد که برای تشخیص قطع از پایان تمیز لازم است.
2. **هدر attach فقط `myRecv` داشت؛ A به `sent` نیاز دارد** (`recv` در لحظه‌ی attach نهایی نیست، و `Promote` فعلی هم count ارسالی را مبادله می‌کند). حالا هر دو آفست مبادله می‌شود.
3. **`upBytes/dlBytes` آفست نیستند** و شمارنده‌های فعلی نسبت به tunnel فعلی‌اند؛ برای چند swap آفست از ابتدای flow + `base` لازم است.
4. **attach دومرحله‌ای (`ACCEPT/REJECT` قبل از freeze)** تا رد شدن طرف مقابل flow را abort نکند.
5. **بودجه‌ی زمان انتقال تا `L`** و رفتار وقتی جایگزین دیر می‌رسد اضافه شد.
6. **انتقال flow نیمه‌بسته**: `ended` فعلی جلوی آن را می‌گیرد؛ به فاز ۱ اضافه شد.
7. **حافظه‌ی B** `flows × K` در هر process است، نه `flows × 2 × K`.
8. **B و restart کلاینت:** flow‌ها به generation بسته‌اند؛ محدوده صریح شد.
9. **فعال شدن A برای همه‌ی flow‌ها** یعنی کنار گذاشتن شرط `promote_bytes`/`legsPerFlow > 1` و معنای فعلی «flowID غیرصفر = promotable»؛ صریح شد و ثبت swapper قبل از شروع پمپ در کلاینت اضافه شد.

## ۱۰. نتیجه‌ی فاز ۰ (اندازه‌گیری)

روش: `e2e/run.sh` (باینری واقعی، loopback، بدون netem، ۴ هسته مشترک با loadgen) با bulk یک GiB، ۳ تکرار برای هر حالت؛ CPU از `/proc/<pid>/stat` (ticks = ۱۰ms) برای مجموع پروسه‌ی سرور و کلاینت. حالت‌ها با knob‌های موجود ساخته شد: «swapper» = `promote_bytes` خیلی بزرگ (پمپ `PumpSwapper` بدون promotion واقعی)، «envelope» = `mux_half_close = true`.

| transport | حالت | throughput (median) | CPU ticks / GiB | rr p50 |
|---|---|---|---|---|
| wsmux | پیش‌فرض | 507 MB/s | ~346 | ~240µs |
| wsmux | `mux_version=2` | 487 MB/s | ~361 | ~240µs |
| wsmux | swapper | 494 MB/s | ~351 | ~240µs |
| wsmux | envelope | 421 MB/s | ~438 | ~240µs |
| wssmux | `mux_version=2` | 378 MB/s | ~496 | ~250µs |
| wssmux | swapper | 377 MB/s | ~494 | ~250µs |
| wssmux | envelope | 326 MB/s | ~583 | ~250µs |

نتیجه‌گیری:
- **مسیر `PumpSwapper` تقریباً رایگان است** (در حد نوسان، حتی روی TLS).
- **envelope هزینه‌ی واقعی دارد:** حدود ۱۴–۱۷٪ افت throughput و ۱۷–۲۶٪ CPU بیشتر (کپی اضافی هر رکورد و رکوردهای ≤۳۲KiB).
- نسبت به آستانه‌ی مالک (۹۵۰ Mbps ≈ ۱۱۹ MB/s) حتی بدترین حالت (۳۲۶ MB/s ≈ ۲.۶ Gbps) حدود ۲.۷ برابر حاشیه دارد. در ۱ Gbps، CPU مجموع سرور+کلاینت با envelope حدود ۰.۷ هسته است (بدون ≈ ۰.۶).
- **تصمیم:** envelope برای همه‌ی flow‌ها پذیرفتنی است. بهینه‌سازی کپی envelope (بدون نوشتن دوباره‌ی payload) به عنوان کار اختیاری بعد از فاز ۲ ثبت می‌شود.

محدودیت‌ها: loopback و بدون netem؛ CPU سرور واقعی (مثلاً VPS دو هسته‌ای) متفاوت است؛ `e2e/syscalls.sh` اجرا نشد، پس تغییر syscall/byte اندازه‌گیری نشده است؛ هر حالت فقط ۳ اجرا.
