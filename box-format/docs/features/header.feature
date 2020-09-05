Feature: Archive header
  The header consists of the following elements:

    * Four (4) LE bytes, which in ASCII represent `\xffBOX` named `magic_bytes`
    * a 32-bit unsigned LE integer field named `version`
    * a 64-bit unsigned LE integer field named `alignment`
    * a 64-bit unsigned LE non-zero integer field named `trailer`

  Scenario: A valid .box header
    Given a valid .box header
    When the header is parsed
    Then a valid header is returned

  Scenario: An invalid header
    Given an invalid header
    When the header is parsed
    Then an error is returned