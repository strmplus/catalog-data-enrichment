import pino from 'pino';
import { PgHelper } from '../util/pg-helper';
import { MongoHelper } from '../util/mongo-helper';

export type NormalizeTitlesServiceProps = {
  logger: pino.Logger;
  pg: PgHelper;
  mongo: MongoHelper;
};

export class NormalizeTitlesService {
  private readonly logger: pino.Logger;
  private readonly pg: PgHelper;
  private readonly mongo: MongoHelper;

  constructor({ logger, pg, mongo }: NormalizeTitlesServiceProps) {
    this.logger = logger.child({ service: NormalizeTitlesService.name });
    this.pg = pg;
    this.mongo = mongo;
  }

  async execute({ imdbId }): Promise<void> {
    this.logger.debug(`${imdbId} title normalization started`);
    const collection = await this.mongo.getCollection('catalog', 'titles');
    const rawTitle = await this.getRawTitle(imdbId);
    this.logger.debug(`${imdbId} title name: ${rawTitle.primarytitle} - ${rawTitle.startyear}`);
    const normalizedTitle = {
      imdbId: rawTitle.tconst,
      primaryTitle: rawTitle.primarytitle,
      originalTitle: rawTitle.originaltitle,
      startYear: rawTitle.startyear,
      endYear: rawTitle.endyear,
      runtimeMinutes: rawTitle.runtimeminutes,
      titleType: rawTitle.titletype,
      isAdult: rawTitle.isadult,
      genres: rawTitle.genres?.split(',') ?? [],
      ratings: [],
    };
    normalizedTitle.ratings = await this.getRawRatings(normalizedTitle.imdbId);
    if (['tvSeries', 'tvMiniSeries'].includes(normalizedTitle.titleType)) {
      const seasons = await this.getRawSeasons(normalizedTitle.imdbId);
      this.logger.debug(`${imdbId} title seasons: ${Object.keys(seasons).length}`);
      Object.assign(normalizedTitle, { seasons });
    }
    await collection.updateOne({ imdbId: normalizedTitle.imdbId }, { $set: normalizedTitle }, { upsert: true });
    this.logger.info(`${imdbId} title normalization completed`);
  }

  private async getRawTitle(imdbId: string) {
    const [row] = await this.pg.query(
      `
      SELECT tconst, primarytitle, originaltitle, startyear, endyear, runtimeminutes, titletype, isadult, genres
      FROM title_basics
      WHERE tconst = $1
      LIMIT 1
    `,
      [imdbId],
    );
    return row;
  }

  private async getRawRatings(imdbId: string) {
    const rows = await this.pg.query(
      `
      SELECT averagerating, numvotes
      FROM title_ratings 
      WHERE tconst = $1
      AND averagerating IS NOT NULL
      AND numvotes IS NOT NULL
      LIMIT 1
    `,
      [imdbId],
    );
    return rows.map((row) => ({
      source: 'IMDB',
      value: row.averagerating,
      votes: row.numvotes,
    }));
  }

  private async getRawSeasons(imdbId: string) {
    const rows = await this.pg.query(
      `
      SELECT te.tconst, te.seasonnumber, te.episodenumber, tb.primarytitle, tb.originaltitle, tb.runtimeminutes
      FROM title_episode te
      JOIN title_basics AS tb ON te.tconst = tb.tconst
      WHERE te.parenttconst = $1
      `,
      [imdbId],
    );
    const seasons = rows.reduce((acc, row) => {
      if (!acc[row.seasonnumber]) {
        acc[row.seasonnumber] = [];
      }
      acc[row.seasonnumber].push({
        imdbId: row.tconst,
        episodeNumber: row.episodenumber,
        primaryTitle: row.primarytitle,
        originalTitle: row.originaltitle,
        runtimeMinutes: row.runtimeminutes,
      });
      return acc;
    }, {});
    return seasons;
  }
}
