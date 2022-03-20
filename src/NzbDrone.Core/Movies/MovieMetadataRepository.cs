using System.Collections.Generic;
using System.Linq;
using NLog;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.Messaging.Events;

namespace NzbDrone.Core.Movies
{
    public interface IMovieMetadataRepository : IBasicRepository<MovieMetadata>
    {
        List<MovieMetadata> FindById(List<int> tmdbIds);
        bool UpsertMany(List<MovieMetadata> data);
    }

    public class ArtistMetadataRepository : BasicRepository<MovieMetadata>, IMovieMetadataRepository
    {
        private readonly Logger _logger;

        public ArtistMetadataRepository(IMainDatabase database, IEventAggregator eventAggregator, Logger logger)
            : base(database, eventAggregator)
        {
            _logger = logger;
        }

        public List<MovieMetadata> FindById(List<int> tmdbIds)
        {
            return Query(x => Enumerable.Contains(tmdbIds, x.TmdbId));
        }

        public bool UpsertMany(List<MovieMetadata> data)
        {
            var existingMetadata = FindById(data.Select(x => x.TmdbId).ToList());
            var updateMetadataList = new List<MovieMetadata>();
            var addMetadataList = new List<MovieMetadata>();
            int upToDateMetadataCount = 0;

            foreach (var meta in data)
            {
                var existing = existingMetadata.SingleOrDefault(x => x.TmdbId == meta.TmdbId);
                if (existing != null)
                {
                    meta.Id = existing.Id;
                    if (!meta.Equals(existing))
                    {
                        updateMetadataList.Add(meta);
                    }
                    else
                    {
                        upToDateMetadataCount++;
                    }
                }
                else
                {
                    addMetadataList.Add(meta);
                }
            }

            UpdateMany(updateMetadataList);
            InsertMany(addMetadataList);

            _logger.Debug($"{upToDateMetadataCount} movie metadata up to date; Updating {updateMetadataList.Count}, Adding {addMetadataList.Count} movie metadata entries.");

            return updateMetadataList.Count > 0 || addMetadataList.Count > 0;
        }
    }
}
