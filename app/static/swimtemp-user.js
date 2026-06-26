/**
 * swimtemp-user.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Handles:
 *   - Fetching current user + saved/liked state on page load (/api/auth/me)
 *   - Rendering save/like buttons into beach cards and the map panel
 *   - Toggling saved/liked state via /api/saves/beach/:slug
 *   - Showing a login prompt when an unauthenticated user tries to save
 *
 * DROP THIS FILE into app/static/swimtemp-user.js
 * LOAD IT in beaches.html and sea-temp-map.html:
 *   <script type="module" src="/static/swimtemp-user.js"></script>
 *
 * The API stubs already return the right shape (user: null, saved: [], liked: [])
 * so this module works today — it just quietly shows no saved state until
 * the auth backend is live.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ── State ─────────────────────────────────────────────────────────────────────

const state = {
    user:  null,          // { id, email, display_name, tier } | null
    saved: new Set(),     // beach slugs
    liked: new Set(),     // beach slugs
    ready: false,
};

const listeners = [];   // callbacks to call when state is hydrated

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function init() {
    try {
        const res  = await fetch('/api/auth/me', { credentials: 'include' });
        const data = await res.json();
        state.user  = data.user || null;
        state.saved = new Set(data.saved || []);
        state.liked = new Set(data.liked || []);
    } catch {
        // Network error or 501 stub — start with empty state, no crash
    }
    state.ready = true;
    listeners.forEach(fn => fn(state));
}

export function onReady(fn) {
    if (state.ready) fn(state);
    else listeners.push(fn);
}

export function getState() { return state; }

init();

// ── Save / Like toggle ────────────────────────────────────────────────────────

/**
 * Toggle saved or liked for a beach slug.
 * If user is not logged in, shows the login prompt and returns false.
 * Returns true if the toggle succeeded.
 */
export async function toggleBeach(slug, action) {
    if (!state.user) {
        showLoginPrompt();
        return false;
    }

    // Optimistic update
    const isLike = action === 'like' || action === 'unlike';
    const set    = isLike ? state.liked : state.saved;
    const adding = action === 'like' || action === 'save';
    adding ? set.add(slug) : set.delete(slug);

    try {
        const res = await fetch(`/api/saves/beach/${slug}`, {
            method:      'POST',
            credentials: 'include',
            headers:     { 'Content-Type': 'application/json' },
            body:        JSON.stringify({ action }),
        });

        if (res.status === 401) {
            // Optimistic update rollback
            adding ? set.delete(slug) : set.add(slug);
            showLoginPrompt();
            return false;
        }

        if (!res.ok) {
            adding ? set.delete(slug) : set.add(slug);
            return false;
        }

        const data = await res.json();
        // Sync to server truth
        data.saved ? state.saved.add(slug) : state.saved.delete(slug);
        data.liked ? state.liked.add(slug) : state.liked.delete(slug);
        return true;

    } catch {
        adding ? set.delete(slug) : set.add(slug);
        return false;
    }
}

// ── Button rendering ──────────────────────────────────────────────────────────

/**
 * Create a save (bookmark) button for a beach slug.
 * Attach to any container — updates its own aria/visual state.
 *
 * Usage in beaches.html card render:
 *   card.appendChild(createSaveBtn(beach.slug));
 *
 * Usage in sea-temp-map.html panel render:
 *   document.getElementById('panel-save-btn-wrap').appendChild(createSaveBtn(slug));
 */
export function createSaveBtn(slug) {
    const btn = document.createElement('button');
    btn.className   = 'st-save-btn';
    btn.dataset.slug = slug;
    btn.setAttribute('aria-label', 'Save beach');
    _updateSaveBtn(btn, slug);

    btn.addEventListener('click', async e => {
        e.preventDefault();
        e.stopPropagation();
        const isSaved = state.saved.has(slug);
        const ok = await toggleBeach(slug, isSaved ? 'unsave' : 'save');
        if (ok) _updateSaveBtn(btn, slug);
    });

    return btn;
}

export function createLikeBtn(slug) {
    const btn = document.createElement('button');
    btn.className    = 'st-like-btn';
    btn.dataset.slug = slug;
    btn.setAttribute('aria-label', 'Like beach');
    _updateLikeBtn(btn, slug);

    btn.addEventListener('click', async e => {
        e.preventDefault();
        e.stopPropagation();
        const isLiked = state.liked.has(slug);
        const ok = await toggleBeach(slug, isLiked ? 'unlike' : 'like');
        if (ok) _updateLikeBtn(btn, slug);
    });

    return btn;
}

function _updateSaveBtn(btn, slug) {
    const active = state.saved.has(slug);
    btn.innerHTML         = active ? '🔖' : '🔖';
    btn.style.opacity     = active ? '1' : '0.4';
    btn.style.filter      = active ? 'none' : 'grayscale(1)';
    btn.setAttribute('aria-pressed', String(active));
    btn.title = active ? 'Remove from saved' : 'Save for later';
}

function _updateLikeBtn(btn, slug) {
    const active = state.liked.has(slug);
    btn.innerHTML         = active ? '❤️' : '🤍';
    btn.setAttribute('aria-pressed', String(active));
    btn.title = active ? 'Unlike' : 'Like this beach';
}

/**
 * Call this after rendering beach cards to wire up all buttons.
 * Looks for data-save-slug attributes — add these to your card HTML now
 * so the wiring is zero-effort when auth ships.
 *
 * In beaches.html card template, add:
 *   <div class="card-actions">
 *     <button class="st-save-btn" data-save-slug="${beach.slug}" aria-label="Save beach">🔖</button>
 *     <button class="st-like-btn" data-like-slug="${beach.slug}" aria-label="Like beach">🤍</button>
 *   </div>
 */
export function wireCardButtons() {
    document.querySelectorAll('[data-save-slug]').forEach(btn => {
        const slug = btn.dataset.saveSlug;
        btn.addEventListener('click', async e => {
            e.preventDefault(); e.stopPropagation();
            const isSaved = state.saved.has(slug);
            const ok = await toggleBeach(slug, isSaved ? 'unsave' : 'save');
            if (ok) _updateSaveBtn(btn, slug);
        });
        _updateSaveBtn(btn, slug);
    });

    document.querySelectorAll('[data-like-slug]').forEach(btn => {
        const slug = btn.dataset.likeSlug;
        btn.addEventListener('click', async e => {
            e.preventDefault(); e.stopPropagation();
            const isLiked = state.liked.has(slug);
            const ok = await toggleBeach(slug, isLiked ? 'unlike' : 'like');
            if (ok) _updateLikeBtn(btn, slug);
        });
        _updateLikeBtn(btn, slug);
    });
}

// ── Login prompt ──────────────────────────────────────────────────────────────

/**
 * Lightweight inline login prompt — no framework needed.
 * When auth is implemented, replace this body with your real modal/drawer.
 * The trigger points (button clicks) are already wired — no frontend changes needed.
 */
function showLoginPrompt() {
    // Remove any existing prompt
    document.getElementById('st-login-prompt')?.remove();

    const el = document.createElement('div');
    el.id = 'st-login-prompt';
    el.innerHTML = `
        <div class="st-login-backdrop"></div>
        <div class="st-login-card" role="dialog" aria-modal="true" aria-label="Sign in to save beaches">
            <button class="st-login-close" aria-label="Close">✕</button>
            <div class="st-login-icon">🌊</div>
            <h2 class="st-login-title">Save your favourite beaches</h2>
            <p class="st-login-body">Create a free account to save and like beaches, and access extended forecasts.</p>
            <form class="st-login-form" id="st-login-form">
                <input type="email" name="email" placeholder="your@email.com"
                       autocomplete="email" required class="st-login-input" />
                <button type="submit" class="st-login-submit">Continue with email →</button>
            </form>
            <p class="st-login-disclaimer">By continuing you agree to our <a href="/privacy">Privacy Policy</a>.</p>
        </div>`;

    // TODO: wire up form submit to /api/auth/register when backend is ready
    el.querySelector('#st-login-form').addEventListener('submit', e => {
        e.preventDefault();
        const email = e.target.email.value;
        // Placeholder — replace with real auth call
        console.log('[SwimTemp] Auth not yet implemented. Email captured:', email);
        el.querySelector('.st-login-title').textContent = 'Check your inbox';
        el.querySelector('.st-login-body').textContent  = `We sent a link to ${email}`;
        el.querySelector('.st-login-form').remove();
    });

    el.querySelector('.st-login-close').addEventListener('click', () => el.remove());
    el.querySelector('.st-login-backdrop').addEventListener('click', () => el.remove());

    document.body.appendChild(el);
}

// ── CSS (injected once) ───────────────────────────────────────────────────────
// Move to a .css file when you have a build step. For now, injected to keep
// this a single drop-in file.

const style = document.createElement('style');
style.textContent = `
    .st-save-btn, .st-like-btn {
        background: none;
        border: none;
        cursor: pointer;
        font-size: 16px;
        padding: 4px;
        line-height: 1;
        transition: transform 0.15s;
    }
    .st-save-btn:hover, .st-like-btn:hover { transform: scale(1.2); }

    /* Card actions container — add to beach card HTML */
    .card-actions {
        position: absolute;
        top: 10px;
        right: 10px;
        display: flex;
        gap: 4px;
        opacity: 0;
        transition: opacity 0.15s;
    }
    .beach-card:hover .card-actions { opacity: 1; }

    /* Login prompt */
    .st-login-backdrop {
        position: fixed; inset: 0;
        background: rgba(13,27,42,0.6);
        z-index: 999;
        backdrop-filter: blur(4px);
    }
    .st-login-card {
        position: fixed;
        top: 50%; left: 50%;
        transform: translate(-50%, -50%);
        z-index: 1000;
        background: #fff;
        border-radius: 20px;
        padding: 32px 28px 24px;
        width: min(420px, 92vw);
        box-shadow: 0 24px 64px rgba(0,0,0,0.18);
        text-align: center;
    }
    .st-login-close {
        position: absolute; top: 14px; right: 16px;
        background: none; border: none; cursor: pointer;
        font-size: 16px; color: #64748b;
    }
    .st-login-icon { font-size: 36px; margin-bottom: 12px; }
    .st-login-title {
        font-family: 'Cormorant Garamond', serif;
        font-size: 22px; font-weight: 600;
        color: #0d1b2a; margin: 0 0 8px;
    }
    .st-login-body {
        font-size: 13px; color: #475569;
        line-height: 1.5; margin: 0 0 20px;
    }
    .st-login-form { display: flex; flex-direction: column; gap: 10px; }
    .st-login-input {
        padding: 12px 16px; border-radius: 10px;
        border: 1px solid rgba(0,0,0,0.15);
        font-size: 14px; font-family: 'DM Mono', monospace;
        outline: none;
    }
    .st-login-input:focus { border-color: #38bdf8; }
    .st-login-submit {
        padding: 12px; border-radius: 10px;
        background: #0d1b2a; color: #fff;
        font-family: 'DM Mono', monospace;
        font-size: 12px; letter-spacing: 0.5px;
        border: none; cursor: pointer;
        transition: background 0.15s;
    }
    .st-login-submit:hover { background: #1a2f45; }
    .st-login-disclaimer {
        font-size: 10px; color: #94a3b8;
        margin: 14px 0 0;
    }
    .st-login-disclaimer a { color: #38bdf8; }
`;
document.head.appendChild(style);
