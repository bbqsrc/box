Feature: Header `magic_bytes` field

  Rule: The magic starting bytes MUST be 'BOX\0'

    Scenario: Valid magic bytes are found
      Given a box file with a valid header
      When magic bytes are parsed
      Then the magic bytes parse successfully

    Scenario: Invalid magic bytes are found
      Given an arbitrary, non-box file
      When magic bytes are parsed
      Then an error regarding missing magic bytes is returned