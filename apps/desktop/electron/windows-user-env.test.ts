import assert from 'node:assert/strict'

import { test } from 'vitest'

import { expandWindowsEnvRefs, parseRegQueryValue, readWindowsUserEnvVar } from './windows-user-env'

// ── parseRegQueryValue ─────────────────────────────────────────────────────

test('parseRegQueryValue extracts a REG_SZ value', () => {
  const out = [
    '',
    'HKEY_CURRENT_USER\\Environment',
    '    CHARTERFORGE_HOME    REG_SZ    F:\\Charterforge\\data',
    ''
  ].join('\r\n')
  assert.equal(parseRegQueryValue(out, 'CHARTERFORGE_HOME'), 'F:\\Charterforge\\data')
})

test('parseRegQueryValue matches the name case-insensitively', () => {
  const out = 'HKEY_CURRENT_USER\\Environment\r\n    Charterforge_Home    REG_EXPAND_SZ    %USERPROFILE%\\h\r\n'
  assert.equal(parseRegQueryValue(out, 'CHARTERFORGE_HOME'), '%USERPROFILE%\\h')
})

test('parseRegQueryValue preserves spaces inside the value', () => {
  const out = '    CHARTERFORGE_HOME    REG_SZ    C:\\Program Files\\Charterforge\r\n'
  assert.equal(parseRegQueryValue(out, 'CHARTERFORGE_HOME'), 'C:\\Program Files\\Charterforge')
})

test('parseRegQueryValue returns null when the value line is absent', () => {
  const out = 'HKEY_CURRENT_USER\\Environment\r\n    Path    REG_SZ    C:\\x\r\n'
  assert.equal(parseRegQueryValue(out, 'CHARTERFORGE_HOME'), null)
  assert.equal(parseRegQueryValue('', 'CHARTERFORGE_HOME'), null)
  assert.equal(parseRegQueryValue('garbage', 'CHARTERFORGE_HOME'), null)
})

// ── expandWindowsEnvRefs ───────────────────────────────────────────────────

test('expandWindowsEnvRefs expands %VAR% case-insensitively', () => {
  assert.equal(expandWindowsEnvRefs('%UserProfile%\\h', { USERPROFILE: 'C:\\Users\\jeff' }), 'C:\\Users\\jeff\\h')
})

test('expandWindowsEnvRefs leaves literal paths and unknown refs intact', () => {
  assert.equal(expandWindowsEnvRefs('F:\\Charterforge\\data', {}), 'F:\\Charterforge\\data')
  assert.equal(expandWindowsEnvRefs('%NOPE%\\x', {}), '%NOPE%\\x')
})

// ── readWindowsUserEnvVar ──────────────────────────────────────────────────

test('readWindowsUserEnvVar returns null off Windows without spawning', () => {
  let spawned = false

  const exec = () => {
    spawned = true

    return ''
  }

  assert.equal(readWindowsUserEnvVar('CHARTERFORGE_HOME', { platform: 'linux', exec }), null)
  assert.equal(spawned, false)
})

test('readWindowsUserEnvVar queries HKCU\\Environment and expands the value', () => {
  const calls = []

  const exec = (cmd, args) => {
    calls.push([cmd, args])

    return 'HKEY_CURRENT_USER\\Environment\r\n    CHARTERFORGE_HOME    REG_EXPAND_SZ    %DRIVE%\\Charterforge\r\n'
  }

  const value = readWindowsUserEnvVar('CHARTERFORGE_HOME', {
    platform: 'win32',
    env: { DRIVE: 'F:' },
    exec
  })

  assert.equal(value, 'F:\\Charterforge')
  assert.deepEqual(calls, [['reg', ['query', 'HKCU\\Environment', '/v', 'CHARTERFORGE_HOME']]])
})

test('readWindowsUserEnvVar returns null when reg exits non-zero (value missing)', () => {
  const exec = () => {
    throw new Error('reg exited 1')
  }

  assert.equal(readWindowsUserEnvVar('CHARTERFORGE_HOME', { platform: 'win32', exec }), null)
})

test('readWindowsUserEnvVar returns null for an empty value', () => {
  const exec = () => '    CHARTERFORGE_HOME    REG_SZ    \r\n'
  assert.equal(readWindowsUserEnvVar('CHARTERFORGE_HOME', { platform: 'win32', exec }), null)
})
