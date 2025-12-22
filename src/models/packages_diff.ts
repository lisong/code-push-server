import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface PackagesDiffInterface extends Model {
    id: number;
    package_id: number;
    diff_against_package_hash: string;
    diff_blob_url: string;
    diff_size: number;
    created_at: Date;
    updated_at: Date;
}

export const PackagesDiff = sequelize.define<PackagesDiffInterface>(
    'PackagesDiff',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        package_id: DataTypes.INTEGER({ length: 10 }),
        diff_against_package_hash: DataTypes.STRING,
        diff_blob_url: DataTypes.STRING,
        diff_size: DataTypes.INTEGER({ length: 10 }),
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'packages_diff',
        underscored: true,
        paranoid: true,
    },
);
