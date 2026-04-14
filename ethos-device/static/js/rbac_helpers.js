/**
 * rbac_helpers.js — Client-side RBAC enforcement
 *
 * Usage in any template:
 *   <script src="/static/js/rbac_helpers.js"></script>
 *
 * 1) Load permissions:
 *      const rbac = await RbacClient.load();
 *
 * 2) Check a single page:
 *      if (rbac.can('register')) { ... }
 *
 * 3) Auto-hide unauthorised nav links:
 *      Add  data-rbac-page="register"  to any element.
 *      The script auto-hides elements the user cannot access.
 *
 * 4) Filter a list of child links under a parent:
 *      const visible = rbac.visibleChildren('user_page');
 */

class RbacClient {
  constructor(data) {
    // ── Snake_case (canonical from API) ──
    this.logged_in      = !!data.logged_in;
    this.emp_id         = data.emp_id  || '';
    this.name           = data.name    || '';
    this.role           = data.role    || 'User';
    this.pages          = new Set(data.pages || []);
    this.is_super_admin = !!data.is_super_admin;
    this.is_admin       = !!data.is_admin;
    this.page_groups    = data.page_groups || {};

    // ── CamelCase aliases (so menu.html etc. work with either style) ──
    this.loggedIn     = this.logged_in;
    this.isSuperAdmin = this.is_super_admin;
    this.isAdmin      = this.is_admin;
    this.empId        = this.emp_id;
    this.pageGroups   = this.page_groups;
  }

  /** Fetch /api/rbac/my_permissions and return an RbacClient instance. */
  static async load() {
    try {
      const r = await fetch('/api/rbac/my_permissions');
      const d = await r.json();
      const client = new RbacClient(d);
      window.__rbac = client;
      return client;
    } catch (e) {
      console.warn('[RBAC] Failed to load permissions:', e);
      const client = new RbacClient({});
      window.__rbac = client;
      return client;
    }
  }

  /** Super Admin always returns true. Others check the allowed set. */
  can(page) {
    if (this.is_super_admin) return true;
    return this.pages.has(page);
  }

  /** Return permitted child page keys for a parent. Super Admin gets all. */
  visibleChildren(parentKey) {
    const children = this.page_groups[parentKey] || [];
    if (this.is_super_admin) return children.slice();
    return children.filter(ch => this.pages.has(ch));
  }

  /** Return true if the parent page should be visible (has any permitted child). */
  canSeeParent(parentKey) {
    if (this.is_super_admin) return true;
    if (this.pages.has(parentKey)) return true;
    const children = this.page_groups[parentKey] || [];
    return children.some(ch => this.pages.has(ch));
  }

  /**
   * Auto-hide/show DOM elements based on data-rbac-page attributes.
   * Super Admin sees everything.
   */
  static async autoEnforce() {
    let rbac = window.__rbac;
    if (!rbac) {
      rbac = await RbacClient.load();
    }

    // Super Admin: show everything
    if (rbac.is_super_admin) {
      document.querySelectorAll('[data-rbac-page]').forEach(el => {
        el.style.display = '';
        el.removeAttribute('data-rbac-hidden');
      });
      document.querySelectorAll('[data-rbac-parent]').forEach(el => {
        el.style.display = '';
        el.removeAttribute('data-rbac-hidden');
      });
      return rbac;
    }

    // Hide individual page elements user cannot access
    document.querySelectorAll('[data-rbac-page]').forEach(el => {
      const page = el.getAttribute('data-rbac-page');
      if (!rbac.can(page)) {
        el.style.display = 'none';
        el.setAttribute('data-rbac-hidden', '1');
      } else {
        el.style.display = '';
        el.removeAttribute('data-rbac-hidden');
      }
    });

    // Hide parent-group elements if no children are accessible
    document.querySelectorAll('[data-rbac-parent]').forEach(el => {
      const parent = el.getAttribute('data-rbac-parent');
      if (!rbac.canSeeParent(parent)) {
        el.style.display = 'none';
        el.setAttribute('data-rbac-hidden', '1');
      } else {
        el.style.display = '';
        el.removeAttribute('data-rbac-hidden');
      }
    });

    return rbac;
  }
}

// Auto-enforce on DOMContentLoaded if any rbac attributes exist
document.addEventListener('DOMContentLoaded', () => {
  if (document.body.hasAttribute('data-rbac-auto') ||
      document.querySelector('[data-rbac-page]') ||
      document.querySelector('[data-rbac-parent]')) {
    RbacClient.autoEnforce();
  }
});
