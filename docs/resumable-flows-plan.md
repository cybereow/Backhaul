# پلن: flow‌های resumable روی wsmux/wssmux

وضعیت: پیشنهاد (هنوز کدی زده نشده). نویسنده: Claude. مبنا: بررسی کد در main (`f4303a8`) و آزمایش‌های اندازه‌گیری روی loopback.

## ۱. مسئله

یک flow (مثلاً SSH) روی یک stream از یک smux session سوار است. smux نمی‌تواند stream را به session دیگر ببرد، پس flow حداکثر تا سقف سخت CDN (`L`) زنده می‌ماند. rotation فعلی (`cdn_max_age`) فقط جلوی stream جدید روی session پیر را می‌گیرد؛ در آزمایش، عمر متوسط flow بلند از ۱۴s (بدون rotation) به ۲۱s رسید (`L=30`) و با ضریب کمتر تا ~۹۰٪ `L`. **هیچ تنظیمی عمر را از `L` بیشتر نمی‌کند.**

## ۲. دو سطح راه‌حل، یک پایه‌ی مشترک

| | سطح A: انتقال برنامه‌ریزی‌شده | سطح B: resume بعد از قطع ناگهانی |
|---|---|---|
| وقتی | در سن rotation، stream قدیمی هنوز سالم است | session بی‌خبر مرد (idle timeout، قطعی شبکه، deploy CDN) |
| نیاز به بافر replay | خیر (stream قدیمی بایت‌های در راه را تحویل می‌دهد) | بله (بایت‌های ack‌نشده) |
| سربارِ همیشگی | تقریباً صفر | بافر حافظه + ack + فریم‌بندی |
| نتیجه | flow از `L` عبور می‌کند | flow از قطع ناگهانی هم جان سالم به در می‌برد |

**تصمیم طراحی کلیدی: هر دو سطح روی یک پروتکل attach ساخته می‌شوند.** تفاوتشان فقط این است که در A آفست‌ها از drain دقیقاً معلوم‌اند، و در B از آفست طرف مقابل + replay. اگر attach از روز اول آفست‌دار و دارای فیلد `mode` باشد، B بعداً فقط بافر و ack اضافه می‌کند و پروتکل بازنویسی نمی‌شود.

## ۳. چیزهایی که از قبل داریم

- `PumpSwapper` (`internal/utils/handlers/promotable_pump.go`): freeze جهت upload در مرز نوشتن، تبادل count دو طرف، نصب tunnel جدید، drain دقیقِ `dlLimit` بایت از tunnel قدیمی. شمارنده‌های `upBytes/dlBytes` همان آفست‌های لازم‌اند.
- کلاینت: `promotableFlows` (flowID به swapper) و `handlePromoteStream`.
- سرور: `dispatchPromotable`، `promoteFlow`، registry جلسه‌ها، `awaitReplacement`/`retireSession`.
- negotiation: الگوی capability (`halfclose-v1`) روی upgrade.
- محدودیت: `PumpSwapper` یک‌بار-مصرف است (`installed`)، و فقط وقتی `promote_bytes > 0` و `mux_version = 2` فعال است.

## ۴. طراحی

### ۴.۱ مدل flow
هر flow resumable یک `flowID` (۶۴ بیتی تصادفی، نه زمان‌محور) و دو شمارنده‌ی یکنواخت دارد: `sent` (بایت commit‌شده به tunnel) و `recv` (بایت تحویل‌شده به app). این‌ها در برابر swap و resume ثابت می‌مانند.

### ۴.۲ پروتکل attach (flow kind جدید، مثلاً `FlowAttach = 0x07`)
هدر: `kind | flowID | mode | myRecv(8)`، و پاسخ طرف مقابل: `peerRecv(8)`.
- `mode = drained`: سطح A. دو طرف stream قدیمی را تا count نهایی drain می‌کنند؛ replay لازم نیست (`peerRecv` فقط تأیید می‌شود).
- `mode = resume`: سطح B. هر طرف از `peerRecv` به بعد را از بافرش replay می‌کند؛ بایت‌های زیر `recv` خودش را دور می‌ریزد (تکراری).
- فقط سرور stream باز می‌کند (OpenStream سمت سرور است)، پس attach همیشه سرور-آغازگر است و race دوطرفه نداریم.
- فقط با capability `resume-v1` فعال می‌شود. بدون آن رفتار فعلی. (دلیل: کلاینت قدیمی در swap دوم قبل از freeze شکست می‌خورد ولی سرور freeze کرده و ۱۰ ثانیه بعد flow را abort می‌کند.)

### ۴.۳ سطح A
1. `PumpSwapper` تعمیم می‌یابد: بعد از هر swap، tunnel جدید `old` می‌شود و state machine برای swap بعدی ریست می‌شود. چون فقط `net.Conn` می‌بیند، flow‌های striped (گروه قدیمی به گروه جدید) هم پوشش داده می‌شوند.
2. بعد از `awaitReplacement`، برای هر flow روی session در حال بازنشستگی: یک stream روی session جایگزین باز می‌شود، attach با `mode=drained`، و `Promote` با build هویتی.
3. `retireSession` تا وقتی swapper‌ها stream قدیمی را نبسته‌اند session را نمی‌بندد (همین الان drain دارد؛ فقط باید مستقل از تعداد stream شمارش کند).
4. شکست قبل از freeze بی‌خطر است (flow روی stream قدیمی می‌ماند). شکست بعد از freeze flow را abort می‌کند.

### ۴.۴ سطح B (فاز بعد، روی همان پروتکل)
- **حالت framed:** payload tunnel به فریم `DATA(len)`, `ACK(off)`, `CLOSE(final)` تبدیل می‌شود تا ack و close صریح داشته باشیم. قطع بدون `CLOSE` یعنی «تعلیق»، نه پایان.
- **بافر replay:** حلقه‌ی `K` بایتی برای هر جهت (مثلاً ۱–۴ MiB، متناسب با BDP). وقتی ack‌نشده به `K` برسد sender block می‌شود (backpressure مثل TCP). حافظه = `تعداد flow × ۲ × K`، پس یک سقف سراسری لازم است؛ flow‌های بیش از بودجه فقط سطح A می‌گیرند.
- **تعلیق و resume:** هر دو طرف state را تا `resume_window` نگه می‌دارند؛ socket محلی باز می‌ماند و app فقط توقف می‌بیند. سرور روی session زنده stream جدید با `mode=resume` باز می‌کند. پس از پایان پنجره، flow بسته می‌شود.
- ack هر N بایت یا هر T میلی‌ثانیه.

### ۴.۵ پیکربندی
هدف: **بدون knob جدید برای A.** با `cdn_max_age > 0` و `mux_version >= 2` و capability سازگار، A خودکار فعال می‌شود. برای B حداکثر یک knob: `resume_window` (ثانیه، `0` = خاموش). بودجه‌ی حافظه مقدار پیش‌فرض داخلی دارد.

## ۵. فازها و معیار پذیرش

**فاز ۰: اندازه‌گیری (اسپایک، بدون merge).** هزینه‌ی مسیر `PromotablePump` برای همه‌ی flow‌ها در برابر مسیر فعلی با `e2e/run.sh` و syscall-count. اگر بیش از آستانه بود، گزینه‌ی فعال‌سازی فقط برای flow‌هایی که از سن مشخصی گذشتند بررسی می‌شود.

**فاز ۱: `PumpSwapper` چندبارمصرف.** معیار: تست property-based (بایت‌های تصادفی، تعداد swap تصادفی، قطع تصادفی بین مراحل) بدون گم‌شدن/تکرار؛ `-race` تمیز.

**فاز ۲: سطح A.** attach + capability + orchestrator + ادغام با `retireSession`. معیار: آزمایش proxy کشنده‌ی سن (همان هارنس قبلی، تبدیل به تست e2e): flow بلند ≥ ۵ برابر `L` زنده بماند و بایت‌ها verify شوند؛ هیچ flow کوتاهی fail نشود.

**فاز ۳: سطح B.** framed mode، بافر، ack، تعلیق/resume، بودجه‌ی حافظه. معیار: تست chaos که connection‌ها را در سن تصادفی (و بدون اخطار) می‌کشد؛ flow‌ها سالم و بایت‌به‌بایت درست می‌مانند؛ سقف حافظه رعایت می‌شود؛ throughput با resume خاموش تغییر نمی‌کند.

**فاز ۴:** README/config، مهاجرت، تست سازگاری با کلاینت قدیمی (rolling upgrade).

## ۶. ریسک‌ها

| ریسک | کاهش |
|---|---|
| باگ در state machine swap (گم‌شدن/تکرار بایت) | property/fuzz test قبل از هر چیز؛ فاز ۱ جدا و مستقل merge شود |
| سربار مسیر swapper برای همه‌ی flow‌ها | فاز ۰ قبل از تعهد |
| abort شدن flow در شکست بعد از freeze | محدود به زمانی که قبلاً هم در `L` قطع می‌شد؛ شمارنده/diag event برای دیده شدن |
| حافظه‌ی بافر در سطح B | سقف سراسری + fallback به سطح A |
| سازگاری با نسخه‌ی قدیمی | capability `resume-v1`، فقط وقتی هر دو طرف اعلام کنند |
| تداخل با half-close (`FlowPlainHC`) | بررسی در فاز ۰/۱؛ promotable‌ها الان full-close‌اند |
| flow‌های UDP | خارج از دامنه |

## ۷. خارج از دامنه
UDP flow‌ها، ws/wss ساده (هر flow یک connection اختصاصی دارد و ذاتاً به `L` محدود است)، و تغییر در smux.

## ۸. سؤال‌های باز (نیاز به تصمیم)
1. **سطح B واقعاً لازم است؟** پیشنهاد: بعد از فاز ۲ با `/diag` و لاگ ببینیم قطع‌های ناگهانی چقدر رخ می‌دهد. اما چون پروتکل attach از اول آفست‌دار است، تصمیم برای B بعداً هزینه‌ی بازنویسی ندارد.
2. بودجه‌ی حافظه‌ی قابل قبول برای بافر replay چقدر است؟
3. همه‌ی flow‌ها resumable شوند یا فقط پورت‌های مشخص (مثلاً SSH)؟
