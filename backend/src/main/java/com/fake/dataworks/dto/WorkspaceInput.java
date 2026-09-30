package com.fake.dataworks.dto;

/** The display name can contain Chinese; code is the stable project identifier. */
public record WorkspaceInput(String name, String code) {}
