#### OWP Open Source Project Template Instructions

1. _Create a new project._
2. _[Copy these files into the new project](#installation)_
3. _Update the README, replacing the contents below as prescribed._
4. _Add any libraries, assets, or hard dependencies whose source code will be included
   in the project's repository to the _Exceptions_ section in the [TERMS](TERMS.md)._
  - _If no exceptions are needed, remove that section from TERMS._
5. _If working with an existing code base, answer the questions on the [open source checklist](opensource-checklist.md)_
6. _Delete these instructions and everything up to the _Project Title_ from the README._
7. _Write some great software and tell people about it._

> Keep the README fresh! It's the first thing people see and will make the initial impression.

## Installation

_To install all of the template files, run the following script from the root of your project's directory:_

```
bash -c "$(curl -s https://raw.githubusercontent.com/NOAA-OWP/owp-open-source-project-template/open_source_template.sh)"
```

----

# Formulation Selection Decision Support (FSDS)

**Description**:  The private repo development of the FSDS tooling. This will eventually migrate to the official formulation-selection OWP repo.

As NOAA OWP builds the model-agnostic NextGen framework, the hydrologic modeling community will need to know how to optimally select model formulations and estimate parameter values across ungauged catchments. This problem becomes intractable when considering the unique combinations of current and future model formulations combined with the innumerable possible parameter combinations across the continent. To simplify the model selection problem, we apply an analytical tool that predicts hydrologic formulation performance (Bolotin et al., 2022, Liu et al., 2022) using community-generated data. The formulation selection decision support (FSDS) tool readily predicts how models might perform across catchments based on catchment attributes. This decision support tool is designed such that as the hydrologic modeling community generates more results, better decisions can be made on where formulations would be best suited. Here we present the baseline results on formulation selection and demonstrate how the hydrologic modeling community may participate in improving and/or using this tool.


Bolotin, L.A., Haces-Garcia F., Liao, M., Liu, Q., Frame, J., Ogden FL (2022). Data-driven Model Selection in the Next Generation Water Resources Modeling Framework. 


Liu et al. (2022) Automated Decision Support for Model Selection in the Nextgen National Water Model. Abstract (H45I-1503) presented at 2022 AGU Fall Meeting 12-16 Dec.

Other things to include:

  - **Technology stack**: python
  - _Indicate the technological nature of the software, including primary programming language(s) and whether the software is intended as standalone or as a module in a framework or other ecosystem._
  - **Status**:  Preliminary development _Alpha, Beta, 1.1, etc. It's OK to write a sentence, too. The goal is to let interested people know where this project is at. This is also a good place to link to the [CHANGELOG](CHANGELOG.md)._
  - **Links to production or demo instances**
  - _Describe what sets this apart from related-projects. Linking to another doc or page is OK if this can't be expressed in a sentence or two._


**Screenshot**: If the software has visual components, place a screenshot after the description; e.g.,

![](https://raw.githubusercontent.com/NOAA-OWP/owp-open-source-project-template/master/doc/Screenshot.png)


## Dependencies

Describe any dependencies that must be installed for this software to work.
This includes programming languages, databases or other storage mechanisms, build tools, frameworks, and so forth.
If specific versions of other software are required, or known not to work, call that out.

## Installation

Detailed instructions on how to install, configure, and get the project running.
This should be frequently tested to ensure reliability. Alternatively, link to
a separate [INSTALL](INSTALL.md) document.

## Configuration

If the software is configurable, describe it in detail, either here or in other documentation to which you link.

## Usage

Show users how to use the software.
Be specific.
Use appropriate formatting when showing code snippets.

## How to test the software

If the software includes automated tests, detail how to run those tests.

## Known issues

Document any known significant shortcomings with the software.

## Getting help

Instruct users how to get help with this software; this might include links to an issue tracker, wiki, mailing list, etc.

**Example**

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.

## Getting involved

This section should detail why people should get involved and describe key areas you are
currently focusing on; e.g., trying to get feedback on features, fixing certain bugs, building
important pieces, etc.

General instructions on _how_ to contribute should be stated with a link to [CONTRIBUTING](CONTRIBUTING.md).


----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)


----
## Credits and references

1. Projects that inspired you
2. Related projects
3. Books, papers, talks, or other sources that have meaningful impact or influence on this project
