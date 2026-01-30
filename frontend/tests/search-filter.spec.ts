// Copyright 2025 StrongDM Inc
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures';
import {
  addContext,
  waitForDebugger,
  waitForDebuggerLoaded,
  getTimelineItems,
  getSearchInput,
} from './utils/assertions';

test.describe('Search and Filter', () => {
  test('search filters timeline to matching turns', async ({
    apiPage,
    goWriter,
    registry,
  }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    // Append 5 turns with varied content
    goWriter.appendTurn(ctx.contextId, 'user', 'Hello assistant');
    goWriter.appendTurn(ctx.contextId, 'assistant', 'Hello! How can I help?');
    goWriter.appendTurn(ctx.contextId, 'user', 'Tell me about CXDB');
    goWriter.appendTurn(ctx.contextId, 'assistant', 'CXDB is an AI context store');
    goWriter.appendTurn(ctx.contextId, 'user', 'Thanks for the info');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Initially should show all 5 turns
    await expect(getTimelineItems(apiPage)).toHaveCount(5);

    // Type "assistant" in search box
    const searchInput = getSearchInput(apiPage);
    await searchInput.fill('assistant');

    // Should filter to turns containing "assistant" (label or content)
    // This includes the 2 assistant turns
    const items = getTimelineItems(apiPage);
    const count = await items.count();
    expect(count).toBeLessThan(5);
    expect(count).toBeGreaterThan(0);
  });

  test('clearing search shows all turns again', async ({
    apiPage,
    goWriter,
    registry,
  }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    goWriter.appendTurn(ctx.contextId, 'user', 'Message 1');
    goWriter.appendTurn(ctx.contextId, 'assistant', 'Response 1');
    goWriter.appendTurn(ctx.contextId, 'user', 'Message 2');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Initially 3 items
    await expect(getTimelineItems(apiPage)).toHaveCount(3);

    // Search to filter
    const searchInput = getSearchInput(apiPage);
    await searchInput.fill('user');

    // Should be filtered
    const filteredCount = await getTimelineItems(apiPage).count();
    expect(filteredCount).toBeLessThanOrEqual(3);

    // Clear search
    await searchInput.fill('');

    // Should show all 3 again
    await expect(getTimelineItems(apiPage)).toHaveCount(3);
  });

  test('Ctrl+K focuses search input', async ({ apiPage, goWriter, registry }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    goWriter.appendTurn(ctx.contextId, 'user', 'Test message');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Press Ctrl+K
    await apiPage.keyboard.press('Control+k');

    // Search input should be focused
    const searchInput = getSearchInput(apiPage);
    await expect(searchInput).toBeFocused();
  });

  test('Cmd+K focuses search input (macOS)', async ({ apiPage, goWriter, registry }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    goWriter.appendTurn(ctx.contextId, 'user', 'Test message');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Press Meta+K (Cmd on macOS)
    await apiPage.keyboard.press('Meta+k');

    // Search input should be focused
    const searchInput = getSearchInput(apiPage);
    await expect(searchInput).toBeFocused();
  });

  test('search is case-insensitive', async ({ apiPage, goWriter, registry }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    goWriter.appendTurn(ctx.contextId, 'user', 'Hello World');
    goWriter.appendTurn(ctx.contextId, 'assistant', 'HELLO THERE');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Search for lowercase "hello"
    const searchInput = getSearchInput(apiPage);
    await searchInput.fill('hello');

    // Should find both turns (case-insensitive)
    const items = getTimelineItems(apiPage);
    const count = await items.count();
    expect(count).toBe(2);
  });

  test('search with no matches shows empty state', async ({
    apiPage,
    goWriter,
    registry,
  }) => {
    const ctx = goWriter.createContext();
    await registry.putBundle('test-bundle-v1');

    goWriter.appendTurn(ctx.contextId, 'user', 'Hello');

    await apiPage.goto('/');
    await addContext(apiPage, ctx.contextId);
    await waitForDebugger(apiPage);
    await waitForDebuggerLoaded(apiPage);

    // Search for something that doesn't exist
    const searchInput = getSearchInput(apiPage);
    await searchInput.fill('xyznonexistent');

    // Should show "No matches" or empty state
    await expect(
      apiPage.locator('[data-context-debugger]').getByText('No matches')
    ).toBeVisible();
  });
});
