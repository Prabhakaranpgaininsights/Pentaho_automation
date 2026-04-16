import time
from playwright.sync_api import sync_playwright

def ask_claude(prompt_text: str) -> str:
    with sync_playwright() as p:

        # ── Launch browser (set headless=False to see it) ──
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page    = context.new_page()

        # ── Open Claude.ai ──
        page.goto("https://claude.ai/new")
        print("⏳ Waiting for page to load...")
        page.wait_for_timeout(3000)

        # ── If not logged in, you must log in manually first ──
        # Save session after first login so you don't repeat it:
        # context = browser.new_context(storage_state="auth.json")

        # ── Find the input box and type prompt ──
        input_box = page.locator('div[contenteditable="true"]').first
        input_box.click()
        input_box.fill(prompt_text)
        print("✅ Prompt pasted")

        # ── Press Enter to send ──
        page.keyboard.press("Enter")
        print("📨 Sent — waiting for response...")

        # ── Wait for Claude to finish responding ──
        # Wait until the "Stop" button disappears (means response is complete)
        page.wait_for_selector(
            'button[aria-label="Stop Response"]',
            state="visible", timeout=10000
        )
        page.wait_for_selector(
            'button[aria-label="Stop Response"]',
            state="hidden", timeout=120000   # wait up to 2 mins
        )
        print("✅ Response complete")

        # ── Copy the last response ──
        # Method 1: Read last message text directly
        messages = page.locator('.font-claude-message')
        last_message = messages.last
        result = last_message.inner_text()

        browser.close()
        return result


# ── Run ──
if __name__ == "__main__":
    prompt = """
    Generate a Pentaho KTR transformation that reads from MySQL
    and writes to PostgreSQL with field mapping.
    """

    response = ask_claude(prompt)
    print("\n══ Claude Response ══")
    print(response)

    # Save to file
    with open("claude_output.txt", "w", encoding="utf-8") as f:
        f.write(response)
    print("\n✅ Saved to claude_output.txt")