import Link from "next/link";

export default function HomePage() {
  return (
    <main>
      <section className="hero">
        <div className="shell hero__grid">
          <div className="hero__content">
            <div className="section-eyebrow">Public-ready analytics platform</div>
            <h1 className="page-title">Turn raw data into dashboards, insights, and model suggestions in minutes.</h1>
            <p className="lead-copy">
              Auto Analytics AI lets anyone upload CSV or Excel files, enter records manually, and automatically receive cleaned data, charts, outlier detection, trend analysis, machine learning recommendations, PDF reports, and shareable links.
            </p>
            <div className="button-row">
              <Link href="/auth" className="button button--primary">
                Launch workspace
              </Link>
              <a href="/sample-datasets/retail-performance.csv" className="button button--secondary">
                Download sample dataset
              </a>
            </div>
          </div>

          <div className="hero-card">
            <div className="hero-card__row">
              <span>Automatic cleaning</span>
              <strong>Missing values, duplicates, data types</strong>
            </div>
            <div className="hero-card__row">
              <span>Visual analytics</span>
              <strong>Bar, line, pie, scatter, heatmap, box plot</strong>
            </div>
            <div className="hero-card__row">
              <span>Machine learning</span>
              <strong>Regression, classification, clustering</strong>
            </div>
            <div className="hero-card__row">
              <span>Reporting</span>
              <strong>PDF export, saved history, public share links</strong>
            </div>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="shell stack stack--xl">
          <div className="section-copy">
            <div className="section-eyebrow">Why teams use it</div>
            <h2>Designed for non-technical users, flexible enough for analysts.</h2>
          </div>
          <div className="feature-grid">
            <article className="feature-card">
              <h3>Upload or type data manually</h3>
              <p>Start with CSV, Excel, or an editable table when you only have a few rows.</p>
            </article>
            <article className="feature-card">
              <h3>Automatic insights</h3>
              <p>See summary statistics, data quality findings, correlations, outliers, and trend analysis without setup.</p>
            </article>
            <article className="feature-card">
              <h3>Model guidance</h3>
              <p>The app detects whether regression, classification, or clustering makes sense and gives baseline evaluation metrics.</p>
            </article>
            <article className="feature-card">
              <h3>Share results instantly</h3>
              <p>Every saved report has a shareable public URL plus a downloadable PDF for stakeholders.</p>
            </article>
          </div>
        </div>
      </section>
    </main>
  );
}

