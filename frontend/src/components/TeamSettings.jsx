import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { Users, Mail, Shield, ShieldCheck, Trash2, UserPlus, Check, AlertCircle, Clock, Crown } from 'lucide-react'

export default function TeamSettings({ authToken }) {
  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {}

  const [org, setOrg]               = useState(null)
  const [members, setMembers]       = useState([])
  const [pending, setPending]       = useState([])
  const [loading, setLoading]       = useState(true)
  const [inviteEmail, setInviteEmail] = useState('')
  const [inviteRole, setInviteRole] = useState('member')
  const [inviting, setInviting]     = useState(false)
  const [inviteStatus, setInviteStatus] = useState(null) // {ok, msg}
  const [error, setError]           = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const r = await axios.get('/api/org', { headers })
      setOrg(r.data.org)
      setMembers(r.data.members || [])
      setPending(r.data.pending_invites || [])
    } catch (e) {
      setError(e?.response?.data?.detail || 'Failed to load team')
    }
    setLoading(false)
  }, [])

  useEffect(() => { load() }, [])

  async function handleInvite(e) {
    e.preventDefault()
    if (!inviteEmail.trim()) return
    setInviting(true)
    setInviteStatus(null)
    try {
      await axios.post('/api/org/invite', { email: inviteEmail.trim(), role: inviteRole }, { headers })
      setInviteStatus({ ok: true, msg: `Invite sent to ${inviteEmail.trim()}` })
      setInviteEmail('')
      load()
    } catch (err) {
      setInviteStatus({ ok: false, msg: err?.response?.data?.detail || 'Failed to send invite' })
    }
    setInviting(false)
  }

  async function handleRoleChange(memberEmail, newRole) {
    try {
      await axios.put(`/api/org/members/${encodeURIComponent(memberEmail)}`, { role: newRole }, { headers })
      setMembers(prev => prev.map(m => m.email === memberEmail ? { ...m, role: newRole } : m))
    } catch (err) {
      alert(err?.response?.data?.detail || 'Failed to update role')
    }
  }

  async function handleRemove(memberEmail) {
    if (!confirm(`Remove ${memberEmail} from the workspace? They will lose access immediately.`)) return
    try {
      await axios.delete(`/api/org/members/${encodeURIComponent(memberEmail)}`, { headers })
      setMembers(prev => prev.filter(m => m.email !== memberEmail))
    } catch (err) {
      alert(err?.response?.data?.detail || 'Failed to remove member')
    }
  }

  const myRole = members.find(m => m.is_you)?.role
  const isAdmin = myRole === 'admin'

  if (loading) return (
    <div className="flex items-center justify-center h-40">
      <div className="w-6 h-6 border-2 border-[#0055A4] border-t-transparent rounded-full animate-spin"/>
    </div>
  )

  return (
    <div className="max-w-2xl mx-auto space-y-6">
      <div>
        <h2 className="text-lg font-bold text-slate-800 mb-1">Team & Access</h2>
        <p className="text-sm text-slate-500">
          Everyone in <span className="font-semibold text-slate-700">{org?.id}</span> shares this workspace.
        </p>
      </div>

      {error && (
        <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-4 py-3">
          <AlertCircle size={14}/> {error}
        </div>
      )}

      {/* Members list */}
      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center gap-2">
          <Users size={15} className="text-[#0055A4]"/>
          <span className="text-sm font-semibold text-slate-700">
            Members <span className="text-slate-400 font-normal">({members.length})</span>
          </span>
        </div>
        <div className="divide-y divide-slate-100">
          {members.map(m => (
            <div key={m.email} className="flex items-center gap-3 px-5 py-3.5">
              <div className="w-8 h-8 rounded-full bg-[#EEF3FF] flex items-center justify-center flex-shrink-0">
                <span className="text-[#0055A4] text-xs font-bold uppercase">
                  {m.email[0]}
                </span>
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-slate-800 truncate">{m.email}</span>
                  {m.is_you && (
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wide">you</span>
                  )}
                </div>
                <div className="text-xs text-slate-400">
                  {m.last_login ? `Last active ${new Date(m.last_login).toLocaleDateString()}` : 'Never signed in'}
                </div>
              </div>
              <div className="flex items-center gap-2 flex-shrink-0">
                {isAdmin && !m.is_you ? (
                  <select
                    value={m.role}
                    onChange={e => handleRoleChange(m.email, e.target.value)}
                    className="text-xs border border-slate-200 rounded-lg px-2 py-1.5 text-slate-600 focus:outline-none focus:border-[#0055A4] bg-white"
                  >
                    <option value="admin">Admin</option>
                    <option value="member">Member</option>
                  </select>
                ) : (
                  <span className={`inline-flex items-center gap-1 text-xs font-semibold px-2.5 py-1 rounded-full ${
                    m.role === 'admin'
                      ? 'bg-[#EEF3FF] text-[#0055A4]'
                      : 'bg-slate-100 text-slate-500'
                  }`}>
                    {m.role === 'admin' ? <Crown size={10}/> : <Shield size={10}/>}
                    {m.role === 'admin' ? 'Admin' : 'Member'}
                  </span>
                )}
                {isAdmin && !m.is_you && (
                  <button
                    onClick={() => handleRemove(m.email)}
                    className="p-1.5 rounded-lg text-slate-300 hover:text-red-500 hover:bg-red-50 transition-colors"
                    title="Remove member"
                  >
                    <Trash2 size={13}/>
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Pending invites */}
      {pending.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-100 flex items-center gap-2">
            <Clock size={15} className="text-amber-500"/>
            <span className="text-sm font-semibold text-slate-700">
              Pending Invites <span className="text-slate-400 font-normal">({pending.length})</span>
            </span>
          </div>
          <div className="divide-y divide-slate-100">
            {pending.map(inv => (
              <div key={inv.email} className="flex items-center gap-3 px-5 py-3.5">
                <div className="w-8 h-8 rounded-full bg-amber-50 flex items-center justify-center flex-shrink-0">
                  <Mail size={13} className="text-amber-500"/>
                </div>
                <div className="flex-1 min-w-0">
                  <span className="text-sm font-medium text-slate-700 truncate block">{inv.email}</span>
                  <span className="text-xs text-slate-400">Invited by {inv.invited_by} · {new Date(inv.sent_at).toLocaleDateString()}</span>
                </div>
                <span className="text-[10px] font-bold text-amber-600 bg-amber-50 px-2 py-1 rounded-full uppercase tracking-wide">
                  Pending
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Invite form — admin only */}
      {isAdmin && (
        <div className="bg-white rounded-xl border border-slate-200 p-5">
          <h3 className="text-sm font-semibold text-slate-700 flex items-center gap-2 mb-4">
            <UserPlus size={14} className="text-[#0055A4]"/> Invite a team member
          </h3>
          <form onSubmit={handleInvite} className="flex gap-2">
            <input
              type="email"
              value={inviteEmail}
              onChange={e => setInviteEmail(e.target.value)}
              placeholder="colleague@company.com"
              required
              className="flex-1 px-3 py-2 rounded-lg border border-slate-200 text-sm text-slate-800
                         placeholder:text-slate-300 focus:outline-none focus:ring-2 focus:ring-[#0055A4]/30
                         focus:border-[#0055A4] transition-all"
            />
            <select
              value={inviteRole}
              onChange={e => setInviteRole(e.target.value)}
              className="px-3 py-2 rounded-lg border border-slate-200 text-sm text-slate-600
                         focus:outline-none focus:border-[#0055A4] bg-white"
            >
              <option value="member">Member</option>
              <option value="admin">Admin</option>
            </select>
            <button
              type="submit"
              disabled={inviting}
              className="px-4 py-2 rounded-lg bg-[#0055A4] hover:bg-[#003d80] disabled:opacity-60
                         text-white text-sm font-semibold transition-all whitespace-nowrap"
            >
              {inviting ? 'Sending…' : 'Send Invite'}
            </button>
          </form>
          {inviteStatus && (
            <p className={`mt-2 text-xs flex items-center gap-1.5 ${inviteStatus.ok ? 'text-emerald-600' : 'text-red-500'}`}>
              {inviteStatus.ok ? <Check size={12}/> : <AlertCircle size={12}/>}
              {inviteStatus.msg}
            </p>
          )}
          <p className="mt-3 text-xs text-slate-400">
            They'll receive a sign-in link by email. Work email addresses only.
          </p>
        </div>
      )}

      {!isAdmin && (
        <div className="bg-slate-50 rounded-xl border border-slate-200 p-5 text-center">
          <ShieldCheck size={20} className="text-slate-300 mx-auto mb-2"/>
          <p className="text-sm text-slate-500">Only admins can invite or manage team members.</p>
        </div>
      )}
    </div>
  )
}
