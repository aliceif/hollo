import { getLogger } from "@logtape/logtape";
import { eq } from "drizzle-orm";
import db from "../db";
import * as schema from "../schema";
import { drive, storageUrlBase } from "../storage";
import type { Uuid } from "../uuid";

const logger = getLogger(["hollo", "cleanup-processors"]);

// Type for thumbnail cleanup item data
interface ThumbnailCleanupItemData {
  id: Uuid;
}

export async function processThumbnailDeletion(
  item: schema.CleanupJobItem,
): Promise<void> {
  const data = item.data as unknown as ThumbnailCleanupItemData;

  const medium = await db.query.media.findFirst({
    where: eq(schema.media.id, data.id),
  });

  if (medium == null) {
    logger.error("medium missing in database: {id}", { id: data.id });
    throw new Error(`medium missing in database: ${data.id}`);
  }

  if (!medium.thumbnailUrl.startsWith(storageUrlBase as string)) {
    logger.error(
      "The thumbnail URL {thumbnailUrl} does not match the storage URL pattern {storageUrlBase}!",
      {
        thumbnailUrl: medium.thumbnailUrl,
        storageUrlBase,
      },
    );
    throw new Error(
      `The thumbnail URL ${medium.thumbnailUrl} does not match the storage URL pattern ${storageUrlBase}!`,
    );
  }

  const key = medium.thumbnailUrl.replace(storageUrlBase as string, "");

  const disk = drive.use();
  await disk.delete(key);
  await db
    .update(schema.media)
    .set({ thumbnailCleaned: true })
    .where(eq(schema.media.id, medium.id));
}
