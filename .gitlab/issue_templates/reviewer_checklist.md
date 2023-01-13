## Review checklist for @dataretrieval

Background information for reviewers [here](https://www.usgs.gov/products/software/software-management/types-software-review)

*Please check off boxes as applicable, and elaborate in comments below.*

- Code location https://code.usgs.gov/cmwsc/shwa/dataretrieval
- author @[gitlab handle]

### Conflict of interest

- [ ] I confirm that I have no COIs with reviewing this work, meaning that there is no relationship with the product or the product's authors or affiliated institutions that could influence or be perceived to influence the outcome of the review (if you are unsure whether you have a conflict, please speak to your supervisor _before_ starting your review).

### Adherence to Fundamental Science Practices

- [ ] I confirm that I read and will adhere to the [Federal Source Code Policy for Scientific Software](https://www.usgs.gov/survey-manual/im-osqi-2019-01-review-and-approval-scientific-software-release) and relevant federal guidelines for approved software release as outlined in [SM502.1](https://code.usgs.gov/cmwsc/shwa/dataretrieval) and [SM502.4](https://www.usgs.gov/survey-manual/5024-fundamental-science-practices-review-approval-and-release-information-products).

### Security Review

- [ ] No proprietary code is included
- [ ] No Personally Identifiable Information (PII) is included
- [ ] No other sensitive information such as data base passwords are included

### General checks

- [ ] **Repository:** Is the source code for this software available?
- [ ] **License:** Does the repository contain a plain-text LICENSE file?
- [ ] **Disclaimer:** Does the repository have the USGS-required provisional Disclaimer?
- [ ] **Contribution and authorship:** Has the submitting author made major contributions to the software? Does the full list of software authors seem appropriate and complete?
- [ ] Does the repository have a code.json file?

### Documentation

- [ ] **A statement of need**: Do the authors clearly state what problems the software is designed to solve and who the target audience is?
- [ ] **Installation instructions:** Is there a clearly-stated list of dependencies? Ideally these should be handled with an automated package management solution.
- [ ] **Example usage:** Do the authors include examples of how to use the software (ideally to solve real-world analysis problems)?
- [ ] **Functionality documentation:** Is the core functionality of the software documented to a satisfactory level (e.g., API method documentation)?
- [ ] **Automated tests:** Are there automated tests or manual steps described so that the functionality of the software can be verified?
- [ ] **Community guidelines:** Are there clear guidelines for third parties wishing to 1) Contribute to the software 2) Report issues or problems with the software 3) Seek support? This information could be found in the README, CONTRIBUTING, or DESCRIPTION sections of the documentation.
- [ ] **References:** When present, do references in the text use the proper [citation syntax](https://pandoc.org/MANUAL.html#extension-citations)?

### Functionality

- [ ] **Installation:** Does installation succeed as outlined in the documentation?
- [ ] **Functionality:** Have the functional claims of the software been confirmed?
- [ ] **Performance:** If there are any performance claims of the software, have they been confirmed? (If there are no claims, please check off this item.)
- [ ] **Automated tests:** Do unit tests cover essential functions of the software and a reasonable range of inputs and conditions? Do all tests pass when run locally?
- [ ] **Packaging guidelines:** Does the software conform to the applicable packaging guidelines? R packaging guidelines [here](https://devguide.ropensci.org/building.html#building); Python packaging guidelines [here](https://packaging.python.org/en/latest/)

### Review Comments

- Add free text comments here.

### Reviewer checklist source statement

This checklist combines elements of the [rOpenSci](https://devguide.ropensci.org/) review guidelines and the Journal of Open Source Science (JOSS) review [checklist](https://joss.readthedocs.io/en/latest/review_checklist.html): it has been modified for use with USGS software releases.
