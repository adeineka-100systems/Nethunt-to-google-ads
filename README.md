# NetHunt → Google Ads (izovata.pro)

Щогодини (GitHub Actions) коригує конверсії **Prom Purchase (Ads tag)** у Google Ads за номером замовлення Prom (`Order id` у NetHunt):

| Подія в NetHunt | Що відбувається в Google Ads |
|---|---|
| Лід у папці **Leads** перейшов у етап **«Відхилено»** | конверсія видаляється (RETRACT) |
| В угоді **В2С угоди**, пов'язаній з лідом, з'явився або змінився **Бюджет** | цінність конверсії = Бюджет (RESTATE) |
| Угода «Втрачена угода» | не видаляється, бюджет передається як звичайно |

Коригування пишуться в аркуш `upload` Google Таблиці; Google Ads забирає його запланованим завантаженням
(Цілі → Конверсії → Завантаження → Розклад). Стан зберігається в аркушах `state` / `meta`, журнал — у `log`.

Обмеження Google: коригування не раніше ніж через 24 год після замовлення, не пізніше 55 днів;
на автостратегії впливають зміни протягом 7 днів.

## Secrets (Settings → Secrets and variables → Actions)

- `NETHUNT_EMAIL` — email користувача NetHunt
- `NETHUNT_API_KEY` — API key NetHunt
- `GOOGLE_SA_JSON` — JSON-ключ сервісного акаунта Google (весь вміст файлу)
- `SHEET_ID` — ID Google Таблиці (з URL між `/d/` і `/edit`); таблицю треба розшарити на email сервісного акаунта з правом редагування

Ручний запуск: вкладка **Actions → NetHunt -> Google Ads adjustments → Run workflow**.
