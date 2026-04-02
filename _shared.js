// poist.ie shared utilities — Supabase auth wired
// v2.0 — localStorage demo auth removed, real Supabase session used throughout

// ── Supabase client ──────────────────────────────────────────────────────────
import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js/+esm';

const SUPABASE_URL = 'https://kpfshjarepmazrgzffot.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtwZnNoamFyZXBtYXpyZ3pmZm90Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUwNzQ0MjIsImV4cCI6MjA5MDY1MDQyMn0.yU-6ASii1duNXgGMs0ZWTkgTYXonVfqVOnEQr0J8FhU';

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ── POIST namespace ──────────────────────────────────────────────────────────
const POIST = {

  // ── Brand ──────────────────────────────────────────────────────────────────
  logo: (dark = false) => `
    <a href="index.html" style="text-decoration:none;font-family:'DM Sans',sans-serif;font-weight:700;font-size:1.65rem;letter-spacing:-0.03em;background:linear-gradient(90deg,${dark ? 'rgba(255,255,255,0.9),rgba(255,255,255,0.6)' : '#0f5a51,#2a9d8f'});-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">poist.ie</a>`,

  // ── Auth helpers ────────────────────────────────────────────────────────────
  //
  // getSession()   → returns the Supabase session object (or null)
  // getUser()      → returns the auth user object (or null)
  // logout()       → signs out and redirects to login
  // requireAuth()  → call at top of any protected page; returns { session, profile }
  //                  or redirects away — never returns null without redirecting
  //
  // Usage in a protected page:
  //   const { session, profile } = await POIST.requireAuth('candidate');
  //   // profile is the row from the candidates / employers table

  getSession: async () => {
    const { data: { session } } = await supabase.auth.getSession();
    return session;
  },

  getUser: async () => {
    const { data: { user } } = await supabase.auth.getUser();
    return user;
  },

  logout: async () => {
    await supabase.auth.signOut();
    window.location.href = 'login.html';
  },

  // requireAuth(role)
  //   role: 'candidate' | 'employer' | 'admin' | null (any authenticated user)
  //   Returns { session, profile } where profile is the DB row for this user.
  //   Redirects to login.html if not authenticated, index.html if wrong role.
  requireAuth: async (role = null) => {
    const session = await POIST.getSession();
    if (!session) {
      window.location.href = 'login.html';
      return null;
    }

    const userId = session.user.id;
    let profile = null;

    if (role === 'candidate' || role === null) {
      const { data } = await supabase
        .from('candidates')
        .select('*')
        .eq('id', userId)
        .single();
      if (data) {
        profile = { ...data, role: 'candidate' };
      }
    }

    if (!profile && (role === 'employer' || role === null)) {
      const { data } = await supabase
        .from('employers')
        .select('*')
        .eq('id', userId)
        .single();
      if (data) {
        profile = { ...data, role: 'employer' };
      }
    }

    // If a specific role was required but profile doesn't match, redirect
    if (role && (!profile || profile.role !== role)) {
      window.location.href = 'index.html';
      return null;
    }

    // If no profile found in either table, something is wrong — back to login
    if (!profile) {
      window.location.href = 'login.html';
      return null;
    }

    return { session, profile };
  },

  // ── CEFR helpers ────────────────────────────────────────────────────────────
  cefrLevels: ['A1', 'A2', 'B1', 'B2', 'C1', 'C2'],
  cefrLabel: {
    A1: 'Beginner',
    A2: 'Elementary',
    B1: 'Intermediate',
    B2: 'Upper-Intermediate',
    C1: 'Advanced',
    C2: 'Proficient/Native'
  },

  // ── Demo seed data (jobs board only — kept for UI until real jobs exist) ────
  demoJobs: [
    { id: 1, title: 'Irish Language Officer', org: 'Department of Education', location: 'Dublin', county: 'Dublin', type: 'Full-time', cefr: 'B2', sector: 'Government', posted: '2 days ago', lat: 53.349, lng: -6.260, desc: 'Lead Irish language strategy across the department, supporting the 2030 Official Languages Act targets.' },
    { id: 2, title: 'Bainisteoir Tionscadail', org: 'Údarás na Gaeltachta', location: 'Galway', county: 'Galway', type: 'Full-time', cefr: 'C1', sector: 'Semi-state', posted: '1 day ago', lat: 53.270, lng: -9.056, desc: 'Manage capital projects across Gaeltacht regions. Full Irish essential.' },
    { id: 3, title: 'Irish Language Teacher', org: 'Coláiste Íde', location: 'Dingle, Kerry', county: 'Kerry', type: 'Full-time', cefr: 'C2', sector: 'Education', posted: '3 days ago', lat: 52.141, lng: -10.267, desc: 'Teach Irish at secondary level at a leading Irish-medium college.' },
    { id: 4, title: 'Communications Officer (Irish)', org: 'HSE', location: 'Cork', county: 'Cork', type: 'Full-time', cefr: 'B2', sector: 'Healthcare', posted: 'Today', lat: 51.898, lng: -8.475, desc: 'Produce Irish-language health communications for HSE Cork region.' },
    { id: 5, title: 'Tech Lead Gaeilge', org: 'GlobalTech Ireland', location: 'Cork', county: 'Cork', type: 'Full-time', cefr: 'B1', sector: 'Technology', posted: '5 days ago', lat: 51.903, lng: -8.468, desc: 'Lead a bilingual engineering team. Irish language skills valued but not essential at native level.' },
    { id: 6, title: 'Content Creator', org: 'TG4', location: 'Galway', county: 'Galway', type: 'Contract', cefr: 'C1', sector: 'Media', posted: 'Today', lat: 53.274, lng: -9.049, desc: 'Create engaging Irish-language video content for TG4 digital platforms.' },
    { id: 7, title: 'Irish Language Coordinator', org: 'Limerick City Council', location: 'Limerick', county: 'Limerick', type: 'Part-time', cefr: 'B1', sector: 'Government', posted: '1 week ago', lat: 52.664, lng: -8.630, desc: 'Coordinate Irish language services for Limerick City Council.' },
    { id: 8, title: 'Community Manager', org: 'Conradh na Gaeilge', location: 'Dublin', county: 'Dublin', type: 'Full-time', cefr: 'C2', sector: 'Non-profit', posted: '4 days ago', lat: 53.342, lng: -6.268, desc: 'Manage community programmes and events promoting the Irish language.' },
  ],

  // demoCandidates kept for employer-search until real search is wired
  demoCandidates: [
    { id: 1, name: 'Síle Ní Bhriain', title: 'Content Creator', location: 'Galway', cefr: 'B2', englishCefr: 'C2', skills: ['Content Writing', 'Public Speaking', 'Media Production'], available: 'Passive', verified: true, avatar: '👩‍💼' },
    { id: 2, name: 'Ciarán Mac Giolla', title: 'Software Developer', location: 'Dublin', cefr: 'C1', englishCefr: 'C2', skills: ['React', 'Node.js', 'Irish Language Tech'], available: 'Active', verified: true, avatar: '👨‍💻' },
    { id: 3, name: 'Aoife Ní Fhaoláin', title: 'Irish Language Officer', location: 'Galway', cefr: 'C2', englishCefr: 'C2', skills: ['Policy', 'Training', 'Communications'], available: 'Passive', verified: true, avatar: '👩‍🏫' },
    { id: 4, name: 'Seán Ó Briain', title: 'Communications Manager', location: 'Cork', cefr: 'B2', englishCefr: 'C1', skills: ['PR', 'Social Media', 'Copywriting'], available: 'Active', verified: false, avatar: '👨‍💼' },
  ],

  // ── Sidebar nav renderer ─────────────────────────────────────────────────────
  renderSidebar: (role, activePage) => {
    const candidateLinks = [
      { href: 'candidate-dashboard.html', icon: '⊞', label: 'Dashboard' },
      { href: 'candidate-jobs.html', icon: '🔍', label: 'Find Jobs' },
      { href: 'candidate-passport.html', icon: '📋', label: 'My Passport' },
    ];
    const employerLinks = [
      { href: 'employer-dashboard.html', icon: '⊞', label: 'Dashboard' },
      { href: 'employer-search.html', icon: '🔍', label: 'Find Candidates' },
      { href: 'employer-post-job.html', icon: '＋', label: 'Post a Job' },
    ];
    const adminLinks = [
      { href: 'admin-dashboard.html', icon: '⊞', label: 'Overview' },
      { href: 'admin-dashboard.html#jobs', icon: '📋', label: 'Job Approvals' },
      { href: 'admin-dashboard.html#users', icon: '👥', label: 'Users' },
    ];
    const links = role === 'employer' ? employerLinks : role === 'admin' ? adminLinks : candidateLinks;
    return `
      <aside style="width:220px;min-height:100vh;background:#0f5a51;display:flex;flex-direction:column;flex-shrink:0;">
        <div style="padding:1.4rem 1.2rem 1rem;border-bottom:1px solid rgba(255,255,255,0.1);">
          ${POIST.logo(true)}
        </div>
        <nav style="padding:0.8rem 0;flex:1;">
          ${links.map(l => `
            <a href="${l.href}" style="display:flex;align-items:center;gap:0.7rem;padding:0.7rem 1.2rem;text-decoration:none;font-size:0.92rem;font-weight:500;border-radius:0;transition:background 0.15s;${activePage === l.href ? 'background:rgba(255,255,255,0.15);color:#fff;' : 'color:rgba(255,255,255,0.7);'}"
               onmouseover="this.style.background='rgba(255,255,255,0.1)'" onmouseout="this.style.background='${activePage === l.href ? 'rgba(255,255,255,0.15)' : 'transparent'}'">
              <span style="font-size:1rem;width:20px;text-align:center;">${l.icon}</span>${l.label}
            </a>`).join('')}
        </nav>
        <div style="padding:1rem 1.2rem;border-top:1px solid rgba(255,255,255,0.1);">
          <button onclick="POIST.logout()" style="background:rgba(255,255,255,0.1);border:none;color:rgba(255,255,255,0.7);padding:0.5rem 1rem;border-radius:6px;cursor:pointer;font-size:0.85rem;width:100%;text-align:left;">← Sign out</button>
        </div>
      </aside>`;
  },

  // ── Top bar for dashboards ───────────────────────────────────────────────────
  renderTopBar: (title, profile) => `
    <div style="height:60px;background:#fff;border-bottom:1px solid #E5E7EB;display:flex;align-items:center;justify-content:space-between;padding:0 1.5rem;flex-shrink:0;">
      <h1 style="font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:700;color:#1A1A2E;">${title}</h1>
      <div style="display:flex;align-items:center;gap:0.8rem;">
        <div style="width:32px;height:32px;border-radius:50%;background:#E8F5F3;display:flex;align-items:center;justify-content:center;font-size:1.1rem;">👤</div>
        <span style="font-size:0.88rem;font-weight:500;color:#374151;">${profile?.full_name || profile?.org_name || 'User'}</span>
      </div>
    </div>`,

  // ── CEFR badge ───────────────────────────────────────────────────────────────
  cefrBadge: (level) => `<span style="background:#F5C518;color:#1A1A2E;font-size:0.7rem;font-weight:700;padding:0.18rem 0.5rem;border-radius:4px;">${level}</span>`,

  // ── Toast notification ───────────────────────────────────────────────────────
  toast: (msg, type = 'success') => {
    const t = document.createElement('div');
    const bg = type === 'success' ? '#1A7A6E' : type === 'error' ? '#dc2626' : '#1A1A2E';
    t.style.cssText = `position:fixed;bottom:1.5rem;right:1.5rem;background:${bg};color:#fff;padding:0.8rem 1.3rem;border-radius:10px;font-size:0.9rem;font-weight:500;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,0.2);animation:slideIn 0.3s ease;`;
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3000);
  },

  // ── Passport completion % ────────────────────────────────────────────────────
  // Checks which fields are filled in the candidate profile row
  passportCompletion: (p) => {
    const fields = ['full_name', 'county', 'cefr_level', 'bio', 'skills', 'speaking_clip_url'];
    const filled = fields.filter(f => p[f] && p[f] !== '' && (!Array.isArray(p[f]) || p[f].length > 0));
    return Math.round((filled.length / fields.length) * 100);
  },
};

window.POIST = POIST;
window.supabase = supabase;