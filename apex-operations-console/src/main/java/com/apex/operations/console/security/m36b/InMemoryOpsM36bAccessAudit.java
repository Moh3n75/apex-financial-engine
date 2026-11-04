package com.apex.operations.console.security.m36b;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Bounded in-process audit for local M36B proof. Not a durable product sink. */
public final class InMemoryOpsM36bAccessAudit implements OpsM36bAccessAudit {
  private static final int CAP = 2_048;
  private final CopyOnWriteArrayList<Entry> entries = new CopyOnWriteArrayList<>();

  @Override
  public void record(Entry entry) {
    entries.add(entry);
    while (entries.size() > CAP) {
      entries.remove(0);
    }
  }

  @Override
  public List<Entry> snapshot() {
    return Collections.unmodifiableList(new ArrayList<>(entries));
  }
}
