# Article_Research_Levels

**Level:** Publication  
**Pipeline notebook:** `Article_Research_Levels.py` (delegates to `basic_applied_classifier_prod_v031_table_creator.py`)  
**Hive table:** `fca_ds.BAC_v031_{YYYYMMDD}` (check exact name in pipeline)

---

## Description

Article-level research-level classification using the BAC model (Klavans and
Boyack framework), implemented via `basic_applied_classifier` in the RADS
library. The model uses article title, abstract, and cited references to
assign each article to one of four levels from applied/clinical to basic.

In reporting language, this indicator is often used to describe a spectrum
from:
- applied/clinical (level 0)
- to basic research (level 3)

General field tendency: physics/chemistry/biology and parts of medicine are
more basic, while engineering/computer science/social sciences and more
clinical medicine are more applied.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Research_Levels')
```

Databricks usage with classifier:

```python
import sys
from pyspark.sql import functions as F

sys.path.append('/Workspace/rads/library')
from basic_applied_classifier import basic_applied_classifier

bac = basic_applied_classifier(snapshot='20240801')
classified_ani = bac.get_or_create_classified_ani()

df_research_levels = (
  classified_ani
  .select('Eid', F.col('research_level').cast('int').alias('research_level'))
  .withColumn(
    'research_level_description',
    F.when(F.col('research_level') == 0, 'Clinical observation / Applied technology')
     .when(F.col('research_level') == 1, 'Clinical mix / Engineering-technological mix')
     .when(F.col('research_level') == 2, 'Clinical investigation / Applied research')
     .when(F.col('research_level') == 3, 'Basic research / Basic scientific research')
     .otherwise(None)
  )
)
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `Eid` | long | Scopus article ID |
| `research_level` | int/double | Numeric class on 0-3 spectrum |

## Label mapping (model output)

| Value | Label |
|---|---|
| `0` | Clinical observation / Applied technology |
| `1` | Clinical mix / Engineering-technological mix |
| `2` | Clinical investigation / Applied research |
| `3` | Basic research / Basic scientific research |

## Methodology notes

- Original journal-level framing traces to Narin, Pinski, Gee (1976).
- Current article-level model follows Boyack et al. (2014), classifying
  individual articles from title/abstract/citation features.
- This avoids the assumption that all papers in a journal share one static
  research level.

## Caveats

- Works best in Physical and Life Sciences.
- Less effective in Social Sciences (model is less trained there).
- Interpret with care in long-tail or cross-disciplinary social science areas.
- Missing text/features may reduce coverage.

---

## Notes

- `Eid` column name is capitalized (not `EID` or `eid`).
- Only articles with title or abstract are classified; others may be absent.
- The BAC v0.31 model is stored at:  
  `/mnt/els/rads-main/mappings_and_metrics/mappings/EID_classifications/BAC/`
- Operational source context (internal):
  `https://elsevier.atlassian.net/wiki/spaces/FCADS/pages/58961270162/Research+Levels`

## References

1. Narin, F., Pinski, G., Gee, H.H. (1976). Structure of the Biomedical
  Literature. *JASIS* 27, 25-45. doi:10.1002/asi.4630270104
2. Boyack, K.W., Patek, M., Ungar, L.H., Yoon, P., Klavans, R. (2014).
  Classification of individual articles from all of science by research level.
  *Journal of Informetrics* 8(1), 1-12. doi:10.1016/j.joi.2013.10.005
